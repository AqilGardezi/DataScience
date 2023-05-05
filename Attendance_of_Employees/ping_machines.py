#!/usr/bin/env python
# coding: utf-8

# Getting all the modules require for this service
import os
import pandas as pd
import numpy as np
import schedule
import smtplib
import io
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.multipart import MIMEBase
from email.utils import COMMASPACE, formatdate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment
from email.mime.image import MIMEImage
from email import encoders
import pymssql
import datetime
#import schedule
import time
import copy

# In[ ]:

# In[8]:

# Function to send messages to relevent persons 
def send_message(Service_Name,Response_Message):
    num={'Waheed':'03465025293', 'Aaqil':'03425469849'}   
    for i in num:
        url="""http://172.16.0.71/TelenorAPI/TelenorSMS.asmx/SendTelenorSMS?RecipentName={0}&PhoneNumbers={1}&SMS_Text=(Alert): {2} Response Msg: {3}&TeamName=DataScience""".format(i,num[i],Service_Name,Response_Message)
        requests.get(url)
        print ("Message Sent to {0}".format(i))


# Function to generate Email
def Generate_Mail(final):
    print('Sending Email...')
    FROM = "email@email.com"
    TO=['email@email.com']
    CC=['demail@email.com']
    SUBJECT="Check-in/out Machines Down"
    server='Smtp.office365.com'
    html_template=''
    # Html Email template
    html_template1= """
    <html>
    <head></head>
    <body>

        <p>Team Networks,<br>
    <br>
    Please check, follwing check-in and check-out machines are down :

        </p>
        <br>	
        <table style="width:40%" cellpadding="4">
        <tr>
        <th style="background:#5b9bd5;color:white;">Machine_IP</th>
        <th style="background:#5b9bd5;color:white;">Check_Type</th>
	<th style="background:#5b9bd5;color:white;">Office</th>
        {% for a in data %}
        <tr style="background:#eaeff7;">
        <td >{{ a[0] }}</td>
        <td >{{ a[1] }}</td>
	<td >{{ a[2] }}</td>

        </tr>{% endfor %}
        </table>
        <br>
        <p>
        THIS IS A SYSTEM GENERATED MAIL BY DATASCIENCE TEAM.
    <p>
        
      </body>
    </html>
    """
    # Data to insert into Email template
    try:
        html_template=html_template1
        msg=MIMEMultipart()
        part1 = MIMEText(Environment().from_string(html_template).render(data = final), "html")
        msg.attach(part1)
        msg['From'] = FROM
        msg['To'] = COMMASPACE.join(TO)
        msg['Cc'] = COMMASPACE.join(CC)
        toAddress = TO+CC
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = SUBJECT
        msg.add_header('Content-Type','text/html')
        smtp = smtplib.SMTP(server, port = 587)
	smtp.starttls()
	smtp.login('email@email.com', 'password')
        smtp.sendmail(FROM, toAddress, msg.as_string())
        smtp.close()
    except:
        return 'Some error occured in sending email.'
    print('Email has been sent.')
    return 0


# Function to check status of Machines
def Check_machines():
    Entry_Terminals=pd.DataFrame()
    try:
        con = pymssql.connect(user='user',password = 'password'
                     ,host='server',database='db',autocommit = True)
    except Exception as e:
        print('unable to make connection')
        send_message('ping machines','unable to make connection')
        con.close()
        return Entry_Terminals
    cur=con.cursor()
# Query to fetch details of check-in and check-out machines of ISB and BAGH
    query = """SELECT DISTINCT MACHINE_IP,
    case
    when office_id = '500' then 'Islamabad'
    when office_id = '507' then 'Bagh'
    End as OFFICE,
    [USER_NAME],[PASSWORD],[PORT],ISNULL(CHECK_TYPE,'NA') as CHECK_TYPE
        FROM TBL_MACHINE WHERE ISNULL(DELETED,0)=0 and office_id in('500','507');
        """
    try:
        cur.execute(query)
        Entry_Terminals=pd.DataFrame(cur.fetchall(),columns=[x[0] for x in cur.description])
    except Exception as e:
        print('Error in query execution')
        send_message('ping machines','Error in query execution')
        con.close()
        return Entry_Terminals
# Getting list of Down Machines
    Down_machines=[]
    final=[]
    for index, row in Entry_Terminals.iterrows():
        response = os.system("ping -c 1 " + row['MACHINE_IP'])
        if response == 0:
            pingstatus = "Network Active"      
        else:
            pingstatus = "Network Error"
            time.sleep(1)
            response = os.system("ping -c 1 " + row['MACHINE_IP'])
            if response == 0:
                pingstatus = "Network Active"      
            else:
                pingstatus = "Network Error"
                time.sleep(1)
                response = os.system("ping -c 1 " + row['MACHINE_IP'])
                if response == 0:
                    pingstatus = "Network Active"      
                else:
                    pingstatus = "Network Error"
                    Down_machines.append(row['MACHINE_IP'])
                    Down_machines.append(row['CHECK_TYPE'])
		    Down_machines.append(row['OFFICE'])
                    final.append(Down_machines)
                    Down_machines=[]
    
# Send Email to concern persons if any machine is down

    if len(final)>0:
        Generate_Mail(final)
            
    else:
	print('total machines: ',Entry_Terminals.shape[0])
        print('No machine down')
    return ''


# Scheduler to run service every 30 minutes
while (True):
    print('Service started...\n')
    Check_machines()
    print('Sleeping for 30 minutes...\n')
    time.sleep(30*60)
    
