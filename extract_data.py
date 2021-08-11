import pymysql as ms
import os
import smtplib
import json
import urllib
import pandas as pd
import boto3
from datetime import date, timedelta

import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(conn_dict,
               receivers_mail_list,
               cc_mail_list, 
               subject="",
               text_message="",
               attach_path="",
               attach_filename=""):
    # Create a message
    message = MIMEMultipart()

    # Details to send it
    message["Subject"] = subject
    message['From'] = conn_dict['mail']
    message['To'] = ", ".join(receivers_mail_list)
    message['Cc'] = ", ".join(cc_mail_list)

    text_message += "\n\nThe Data Operator of Citygo"

    message.attach(MIMEText(text_message,"plain"))
    
    # Attachment
    with open(attach_path) as attach_file:
        attachment = MIMEText(attach_file.read(), _subtype='csv', _charset='utf-8')
    #add header with filename
    attachment.add_header("Content-Disposition", "attachment", filename=attach_filename)
    message.attach(attachment)

    # Create a secure SSL context for connection to SMTP
    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(conn_dict['smtp'],conn_dict['port'], context=context) as server:
        server.login(conn_dict['mail'], conn_dict['password'])
        server.sendmail(
            conn_dict['mail'], receivers_mail_list+cc_mail_list, message.as_string()
        )

    return "e-mail sent!"

def get_secret_value(secret_name=""):

    region_name="eu-west-3"
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']    # name of the csv to create 
            secret_json = json.loads(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret_json = json.loads(decoded_binary_secret)
            
    return secret_json

def lambda_handler(event, context): 
    # Parameters
    today = date.today().strftime("%Y-%m-%d")
    one_week_before = (date.today() - timedelta(days = 7)).strftime("%Y-%m-%d") 
    print(today, one_week_before)
    
    # DB connection
    ## Get SSL certificate
    url = "https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem"
    file = urllib.request.urlopen(url)
    ca_text = file.read().decode("utf-8")
    ca_path = '/tmp/ssl_ca'
    
    with open(ca_path, 'w+') as f:
        f.write(ca_text)
        
    ## Get DB credentials
    param_mysql = get_secret_value("dbsecretname")
    
    print("Building connection..")
    connection = ms.connect(host=param_mysql['host'], 
                            user=param_mysql['username'],
                            password=param_mysql['password'],
                            database=param_mysql['dbname'],
                            port=int(param_mysql['port']),
                            ssl={'ca': ca_path})
    print("Connection established")
    
    # Extract data & save it to tmp
    # query to send
    sql_query = """ SELECT a.id,
                    b.* 
                    FROM `reservation_a` a
                    join `reservation_b` b on (a.id = b.id)
                    where a.date > '{one_week_before}' """.format(one_week_before=one_week_before)
    result_df = pd.read_sql(sql_query, connection)
    print(result_df.head())
    # name of the csv to create
    csv_name = date.today().strftime("%d%m%Y")+"_reservation_chat.csv"
    path= '/tmp/{csv_name}'.format(csv_name=csv_name)
    result_df.to_csv(path, sep = ';', index=False)  
    
    # Mail 
    ## List of receivers
    list_receivers_dict = {
        'prod' : ['fixme@fix.me'],
        'test' : ['kf@citygo.me']
    }
    ## CC list
    list_cc_dict = {
        'prod' : ['fixme@fix.me','fixme@fix.me'],
        'test' : ['kf@citygo.me']
    }
    ## Test or Prod?
    receivers_mail_list = list_receivers_dict['test']
    cc_mail_list = list_cc_dict['test']
    ## Sender
    param_mail = get_secret_value('mailsecretname')
    ## Create mail
    subject = "Extract Data"
    message = "Hello, Extract from "+ one_week_before +" to "+ today
    ## Send it
    send_email(param_mail, receivers_mail_list, cc_mail_list, subject, message, path, csv_name)
    
    return 0
