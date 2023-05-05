import requests
from datetime import datetime
import numpy as np
import threading
import time
import urllib
import urllib3
import pandas as pd
import os
from requests.auth import HTTPProxyAuth
#from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
from dateutil.parser import parse
from sqlalchemy import create_engine, event
import pymssql
import sqlalchemy
import sqlalchemy as db
import warnings
warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import timedelta


try:
    output_engine = pymssql.connect(user = 'user' , password = 'password', host = 'server', database = 'db', appname = 'datascience_ddp', autocommit = True)
    output_connection = output_engine.cursor()
except:
    print ("Unable to make connection")


try:
    output_connection.execute("""

        IF OBJECT_ID('TEMPDB..#temp') IS NOT NULL           
        DROP TABLE #temp
        select * into #temp from(
        select employee_id,check_type,check_date_time, rowguid,created_by,
        row_number() over (PARTITION BY Employee_id Order by check_date_time desc) as rn
        from emp_attendance 
        where datediff(day, created_date,getdate())<=3  and isnull(deleted,0)!=1) a where rn=1 

        --getting terminal server logs of log-in/out

        IF OBJECT_ID('TEMPDB..#temp1') IS NOT NULL           
        DROP TABLE #temp1
        select * into #temp1 from(
        select [user],emp_id,[time],[action], remote_active_id,
        row_number() over (PARTITION BY emp_id Order by [time] desc) as rn1
        from ds_remote_users_active_log
        where datediff(day, time,getdate())<=3
    ) q --where rn1=1 

        --select * from #temp1 where emp_id=8548

        --add these employees checkin
        IF OBJECT_ID('TEMPDB..#temp3') IS NOT NULL           
        DROP TABLE #temp3
        select emp_id,[action] , [time] as check_in,check_type,check_date_time, rowguid,remote_active_id 
        into #temp3
        from #temp t inner join #temp1 t1 on t.employee_id=t1.emp_id and t1.[time] > t.check_date_time
        where (t.check_type='O' and t1.[action] like '%on%' and [time]>check_date_time) or 
        (t.check_type='I' and t.created_by LIKE '%FRONT_CAMERA%' and t1.[action] like '%on%' and [time]>check_date_time)
        or 
        (t.check_type='I' and t1.[action] like '%on%' and [time]>check_date_time)

        select emp_id,check_in
        from #temp3
        where check_in>'2020-08-31 00:00:00' 
        """)
    df_check_ins=pd.DataFrame(output_connection.fetchall(),columns=[x[0] for x in output_connection.description])

    # print(df_check_ins)

    output_connection.execute("""

    IF OBJECT_ID('TEMPDB..#temp') IS NOT NULL           
    DROP TABLE #temp
    select * into #temp from(
    select employee_id,check_type,check_date_time, rowguid,created_by,
    row_number() over (PARTITION BY Employee_id Order by check_date_time desc) as rn
    from emp_attendance 
    where datediff(day, created_date,getdate())<=3  and isnull(deleted,0)!=1) a where rn=1

    --Select * from #temp
    --getting terminal server logs of log-in/out

    IF OBJECT_ID('TEMPDB..#temp1') IS NOT NULL           
    DROP TABLE #temp1
    select * into #temp1 from(
    select [user],emp_id,[time],[action], remote_active_id,
    row_number() over (PARTITION BY emp_id Order by [time] desc) as rn1
    from ds_remote_users_active_log
    where datediff(day, time,getdate())<=3
    ) q --where rn1=1 


    --add these employees checkout

    IF OBJECT_ID('TEMPDB..#temp4') IS NOT NULL           
    DROP TABLE #temp4
    select emp_id,[action] , [time] as check_out,check_type,check_date_time,rowguid,remote_active_id 
    into #temp4
    from #temp t inner join #temp1 t1 on t.employee_id=t1.emp_id and t1.[time] > check_date_time and datediff(minute, [time],getdate())>=36
    where (t.check_type='I' and t1.[action] like '%of%' and [time]>check_date_time and t.created_by not like '%front%') or 
    (t.check_type='O' and t.created_by LIKE '%FRONT_CAMERA%' and t1.[action] like '%of%' and [time]>check_date_time)

    or
    (t.check_type='O' and t1.[action] like '%of%' and [time]>check_date_time)

    --select checkouts for ds_tbl_employee_attendance
    select emp_id,check_out
    from #temp4
    where check_out>'2020-08-31 00:00:00'
        """)
    df_check_outs=pd.DataFrame(output_connection.fetchall(),columns=[x[0] for x in output_connection.description])


    output_connection.execute("""Select * from ds_tbl_employee_attendance  
    where datediff(minute, convert(datetime,event_time),getdate()) <= 8 order by Event_Time desc
    """)

    df_old_data = pd.DataFrame(output_connection.fetchall(),columns=[x[0] for x in output_connection.description])

    # print('df_old_data')
    # print(df_old_data.head())

    # print(df_check_outs)
    output_connection.close()
    output_engine.close()

    df_check_outs.rename(columns={ 'emp_id':'card_no','check_out':'event_time'},inplace=True)
    df_check_outs['event_type']='Face Authentication Passed'
    df_check_outs['card_holder']= df_check_outs['card_no']
    df_check_outs['direction']='O'
    df_check_outs['event_source']='Remote Service'

    # print(df_check_outs)


    df_check_ins.rename(columns={ 'emp_id':'card_no','check_in':'event_time'},inplace=True)
    df_check_ins['event_type']='Face Authentication Passed'
    df_check_ins['card_holder']= df_check_ins['card_no']
    df_check_ins['direction']='I'
    df_check_ins['event_source']='Remote Service'

    # print(df_check_ins)

    frames=[df_check_ins,df_check_outs]

    result = pd.concat(frames,ignore_index=True)

    # print(result)

    result = result.sort_values(by=['card_holder','event_time'],ascending=[True,False]).reset_index(drop=True)

    result[['event_time_next',"direction_next"]] = (result.sort_values(by=['event_time'], ascending=True)
                    .groupby(['card_no'])['event_time',"direction"].shift(-1))

    result["event_time"] = result["event_time"].astype('datetime64[ns]')
    result["event_time_next"] = result["event_time_next"].astype('datetime64[ns]')

    result['direction_next']=result['direction_next'].mask(pd.isnull, '')

    # print(result)

    result_group = result.groupby('card_no')['event_time'].min().reset_index()

    # print(result_group)

    result_valid =  result[((result["direction"]=='O') & (result["direction_next"]=='I') & 
                    (result["event_time"]+timedelta(minutes = 35)<=result["event_time_next"])) | 
                ((result["direction"]=='O') & (result["direction_next"]=='') & 
                (result["event_time"]+timedelta(minutes = 35)<=datetime.now())) 
                    ] 



    missing_entries = result_group.merge(result.reset_index(drop = True), on = ["card_no", "event_time"])

    missing_entries = missing_entries[((missing_entries['direction'] == 'I') & (missing_entries['direction_next'] == 'O'))|
                                    ((missing_entries['direction'] == 'I') & (missing_entries['direction_next'] == ''))
                                    ]

    result_valid = result_valid.append(missing_entries, ignore_index = True)

    # print(result_valid)


    result_valid_copy = pd.DataFrame(columns = result_valid.columns[:-2])
    for index,row in result_valid.iterrows():
        df_len = result_valid_copy.shape[0]
        if not pd.isnull(row['event_time_next']) and row['direction_next'] == 'I' :
            result_valid_copy.loc[df_len] = [row['card_no'], row['event_time_next'], row['event_type'],
                                        row['card_holder'], row['direction_next'], row['event_source']]
            df_len = result_valid_copy.shape[0]
            result_valid_copy.loc[df_len] = [row['card_no'], row['event_time'], row['event_type'],
                                        row['card_holder'], row['direction'], row['event_source']]
            continue
        result_valid_copy.loc[df_len] = [row['card_no'], row['event_time'], row['event_type'],
                                    row['card_holder'], row['direction'], row['event_source']]


    result_valid_copy = result_valid_copy.sort_values(by=['card_holder','event_time'],ascending=[True,False]).reset_index(drop=True)

    result_valid_copy['event_time'] = pd.to_datetime(result_valid_copy['event_time'], format='%Y:%m:%d:%H:%M:%S')


except Exception as e:
    print(e)


#     if df_old_data.shape[0]>0:
#         df_old_data.columns=df_old_data.columns.str.lower()
#         df_old_data['event_time'] = pd.to_datetime(df_old_data['event_time'], format='%Y-%m-%d %H:%M:%S')
#         final = pd.merge(result_valid_copy,df_old_data,how='left', on= ['direction', 'event_time', 'card_no', 'event_source'],indicator=True)
#         final = final[final['_merge']=='left_only']
#         final.drop(['_merge'],axis=1,inplace=True)
#     else:
#         final=result_valid_copy

#     final=final.drop_duplicates(keep=False)
#     print("Final data to insert", final.shape[0])


#while(True):
try:
	engine = db.create_engine('mssql+pymssql://user:password@server/db')
	con=engine.connect()
except:
	print('Unable to connect to DB')
	con.close()
	engine.dispose()
try:
	result_valid_copy.to_sql('ds_tbl_employee_attendance',con, if_exists='append', index=False)
	print('Data inserted: ', result_valid_copy.shape[0])
except Exception as e:
	print('Error in data Insertion', e)
con.close()
engine.dispose()

#    print("Sleeping for five hours....")
#    print(".......................\n")
#    time.sleep(5*60*60)


#print('Calling Terminal Switching Issue Resolver Service....')
#try:
#   subprocess.call('python Terminal_Switching_Issues.py',shell=True)
#  print('Completed.\n')
#except:
#    print('Error in Terminal Switching service')
#     time.sleep(1*60)

