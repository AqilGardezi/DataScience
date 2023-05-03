#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import json
import pymssql
from datetime import datetime as  dt, timedelta
import pyodbc
import sqlalchemy as sa
import urllib
from sqlalchemy import create_engine
from urllib.request import pathname2url
from flask import Flask


# In[11]:


# def incentive():
def get_data():
    conn = pymssql.connect(user='username',password = 'password'
                 ,host='server',database='db',autocommit = True)
    cur = conn.cursor()

    query = '''select EmployeeId,Target, [type] as [Function],StartDate ,EndDate  from GP_WEB_TBL_TaskForcIncentiveEmployees
                where datediff(day, StartDate, GETDATE()) <= 60
                '''
    cur.execute(query)
    row = cur.fetchall()
    df = pd.DataFrame(row,columns=[x[0] for x in cur.description])
    cur.close()
    conn.close()
    return df


# df = get_data()

def weekly_data_split(df):
    try:
        weekmask = "Mon Tue Wed Thu Fri Sat"
        dict1 = {}

        column_name=['employee_id','dates','target', 'Function']
        df1=pd.DataFrame(columns = column_name)

        for idx, rows in df.iterrows():
            no_of_dates = pd.bdate_range(start=rows['StartDate'], end = rows['EndDate'], freq="C", weekmask = weekmask)

#             df2 = pd.DataFrame((no_of_dates), columns=['dates'])

            if len(no_of_dates) > 1:
                tar = rows['Target']/len(no_of_dates)

            else:
                tar = rows['Target']

            for i in no_of_dates:
                a_dict = {'employee_id':rows['EmployeeId'],'dates': i ,'target' : tar, 'Function':rows['Function']}

                df2 = pd.DataFrame(a_dict, index=[0])
                df1 = pd.concat([df1, df2], ignore_index = True)     
        return df1
    except Exception as e:
        print("Exception: pre_processing",e)
        pass



def get_signoff():
    try:
        conn = pymssql.connect(user='username',password = 'password'
                 ,host='server',database='db',autocommit = True)
        cur = conn.cursor()

        query = '''
                select EmployeeId,SignOffDate,TargetAchieved,
                CASE 
                    when audit <= TargetAchieved
                    then audit
                    else
                    TargetAchieved
                END as 
                Audit_Performed, Error from GP_WEB_TBL_TaskForceIncentive_WeeklySignoff
                where datediff(day, SignOffDate, GETDATE()) <= 60
        '''
        cur.execute(query)
        row = cur.fetchall()
        df = pd.DataFrame(row,columns=[x[0] for x in cur.description])
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print("Exception: get_signoff ",e)
        pass


def calculate_inc(df1,df2):
    df1['employee_id'] = df1['employee_id'].astype(int)
    df1['dates'] = pd.to_datetime(df1['dates'])
    df2['EmployeeId'] = df2['EmployeeId'].astype(int)
    df2['SignOffDate'] = pd.to_datetime(df2['SignOffDate'])


    df3 = df1.merge(df2,left_on=['employee_id','dates'], right_on=['EmployeeId','SignOffDate'],how='left')


    df3.fillna(0).head()


    Final = df3.drop(["EmployeeId","SignOffDate"], axis=1)

    Final['Day_of_Week'] = Final['dates'].dt.weekday
    Final['Month_of_Year'] = Final['dates'].dt.month
    Final['Week_Of_Year'] = Final['dates'].dt.week
    Final['Week_Of_Month'] = Final['dates'].apply(lambda x: x.isocalendar()[1] - x.replace(day=1).isocalendar()[1] )


    Final['RN'] = Final.sort_values(['dates','Day_of_Week'], ascending=[True,True]).groupby(['employee_id','Week_Of_Year']).cumcount() + 1

    # Final['Az']=Final['dates'] -  pd.to_timedelta(Final['Day_of_Week'], unit='d').where((Final['Day_of_Week'] != 0) & (Final['RN'] == 1))
    Final[Final.employee_id== 11275]

    Final = Final.sort_values(by=['employee_id','RN'])


    Final['missing_monday']=Final['dates'] -  pd.to_timedelta(Final['Day_of_Week'], unit='d').where((Final['Day_of_Week'] != 0) & (Final['RN'] == 1))

    Final['start_date'] =Final.apply(lambda row: row.dates if row.Day_of_Week==0 else row.missing_monday, axis=1)
    Final['end_date'] = Final['start_date'] + pd.offsets.Week(weekday=5)


    Final1 = Final.groupby(["employee_id","Week_Of_Year"]).first().reset_index()


    final=Final.merge(Final1,on=["employee_id","Week_Of_Year"],how='left')



    final.drop(["target_y","Function_y","start_date_x","end_date_x","dates_y","TargetAchieved_y","missing_monday_y","missing_monday_x","RN_x", "RN_y","Day_of_Week_y","start_date_x","Month_of_Year_y","Week_Of_Month_y","Audit_Performed_y","Error_y"], axis=1, inplace = True)


    fianl = final.rename(columns={"employee_id": "EMPLOYEE_ID", "target_x": "TARGET","dates_x": "DATE","Function_x":"FUNCTION", 
                        "TargetAchieved_x": "SIGN_OFF","Audit_Performed_x": "AUDIT","Error_x":"ERROR", "Day_of_Week_x": "DAY_OF_WEEK","Month_of_Year_x":"MONTH_OF_YEAR",
                        "Week_Of_Year":"WEEK_OF_YEAR","Week_Of_Month_x":"WEEK_OF_MONTH",'start_date_y':"START_DATE","end_date_y":"END_DATE"},inplace=True)


    final["TARGET"] = final.TARGET

    final1 = final.groupby(['EMPLOYEE_ID','START_DATE','END_DATE',"FUNCTION"]).agg({'SIGN_OFF':'sum','TARGET': 'sum', 'AUDIT':'sum', 'ERROR':'sum'},inplace=True).reset_index()

    #         final1 = final1.groupby(['EMPLOYEE_ID','START_DATE','END_DATE']).agg({'SIGN_OFF':'sum','TARGET': 'sum', 'AUDIT':'sum', 'ERROR':'sum','FUNCTION': ','.join},inplace=True).reset_index()

    final1["PRODUCTIVITY_%"] = (final1["SIGN_OFF"]/final1["TARGET"])*100

    final1["INCENTIVE_%"] = final1.apply(lambda x: 100 if x['PRODUCTIVITY_%'] >= 100 else 
                                        (90 if x['PRODUCTIVITY_%'] >90 and x['PRODUCTIVITY_%']<99.9 else 
                                        (70 if x['PRODUCTIVITY_%'] > 80 and x['PRODUCTIVITY_%']<89.9 else 0)),axis=1)



    final1["PRODUCTIVITY_SCORE"] = final1.apply(lambda y: 40 if y['INCENTIVE_%'] == 100 else 
                                        (36 if y['INCENTIVE_%'] == 90 else
                                        (28 if y['INCENTIVE_%'] == 70 else 0)),axis=1)



    final1["ERROR_PERCENTAGE"] = ((final1['ERROR']/final1['AUDIT'])*100).round(1)


    final1["ACCURACY_PERCENTAGE"] = (100-final1['ERROR_PERCENTAGE']).round(1)


    final1['ACCURACY_SCORE'] = final1['ACCURACY_PERCENTAGE'].apply(lambda x:20 if x == 100 
                                                                    else(18 if x>=98.0 and x<=99.9
                                                                    else(16 if x>=95 and x <=97.99
                                                                        else 0)))
    final1.rename(columns = {"FUNCTION":'TYPE'},inplace=True)
    return final1

def discipline_knowledge():
    conn = pymssql.connect(user='username',password = 'password'
                 ,host='server',database='db',autocommit = True)

    cur = conn.cursor()

    query = """
        select Employee_id, (User_FName +' '+ User_LName) as EMPLOYEE_NAME
        --, usa.KnowledgeStatus,usa.DisciplineStatus,usa.Start_date,usa.End_date
        --from UserSurvey_automation usa
        from users where Department_Id = 5002 and Employee_id is not null
        --and datediff(day, usa.Start_date, GETDATE()) <= 30

            """
    cur.execute(query)
    output = cur.fetchall()
    new_conn = pd.read_sql(query, con = conn)
    conn.close()
    cur.close()
    return new_conn


def final_data(final1,emp_name):
    emp_name['Employee_id'] = emp_name['Employee_id'].apply(int)

    final12 = final1.merge(emp_name,left_on=['EMPLOYEE_ID'], right_on=['Employee_id'],how='left')

    final12.drop(["Employee_id"], axis=1, inplace = True)

    final12 = final12[['EMPLOYEE_ID','EMPLOYEE_NAME', 'START_DATE','END_DATE',"TYPE",'PRODUCTIVITY_SCORE','ACCURACY_SCORE','AUDIT','ERROR','ERROR_PERCENTAGE','ACCURACY_PERCENTAGE']]
    return final12

def dump_data(df):
    try:
        conn = pymssql.connect(server='172.16.0.168', user='mtbcweb', password='mtbcweb@mtbc', database='global_portal')
        cur = conn.cursor()
        for index, row in final12.iterrows(): 
            cur.execute("INSERT INTO DS_TASK_FORCE_INCENTIVE (EMPLOYEE_ID,EMPLOYEE_NAME,START_DATE,END_DATE,TYPE,PRODUCTIVITY_SCORE,ACCURACY_SCORE,AUDIT,ERROR,ERROR_PERCENTAGE,ACCURACY_PERCENTAGE) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)",(row['EMPLOYEE_ID'], row['EMPLOYEE_NAME'],row['START_DATE'], row['END_DATE'], row['TYPE'],row['PRODUCTIVITY_SCORE'], row['ACCURACY_SCORE'],row['AUDIT'], row['ERROR'], row['ERROR_PERCENTAGE'], row['ACCURACY_PERCENTAGE']))
        conn.commit()
        conn.close()
        cur.close()
    except Exception as e:
        print("Error in data dumping",e)
    return


# In[12]:


def main():
    df = get_data()
    df1 = weekly_data_split(df)
    df1 = df1.groupby(['employee_id','dates']).agg({'target': 'sum','Function': ','.join},inplace=True).reset_index()
    df2 = get_signoff()
    final1 = calculate_inc(df1,df2)
    emp_name = discipline_knowledge()
    final12 = final_data(final1,emp_name)
#     dump_data(final12)
    return final12



abc = main()




# def dump_data(df):
#     try:
#         conn2 =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=db;UID=user;PWD=password;APP=Datascience_ddp"
#         quoted = pathname2url(conn2)
#         new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
#         engine = create_engine(new_con)
#         df.to_sql('DS_TF_Incentive', engine, if_exists='append',index=False, chunksize = None)
#         print("Data Dumped Successfully")
#     except Exception as e:
#         print("Error in data dumping",e)

#     print("Calling data dumping function....", dump_data(df))
#     return "Data Dumped to DB"

