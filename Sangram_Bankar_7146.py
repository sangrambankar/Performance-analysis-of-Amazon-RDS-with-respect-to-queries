'''
Created on Jun 14, 2016

@author: Sangram
'''
from flask import Flask,render_template,request
import os
import boto
import sys
import csv
from boto.s3.key import Key
import boto.s3.connection
import time
import mysql
import mysql.connector
import random
from _csv import reader
from StringIO import StringIO
import string
import os
import re
import sys
import csv
import time
import argparse
import collections
import warnings
import mysqlcsv
# suppress annoying mysql warnings
from werkzeug.utils import secure_filename

app = Flask(__name__)



UPLOAD_FOLDER = ''
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


#uploading to the bucket
access_key = ''
secret_key = ''
filename= "dummy1.csv"


#connecting to rds instance
cloud6331.cygo3cs6hcoz.us-west-2.rds.amazonaws.com',
    'username': '',
      'password': ''}
user=''
password=''
host=''
database=''

connection = mysql.connector.connect(user=user, password=password,host=host,database=database)
cursor = connection.cursor()
sql = "USE cloudDB"
cursor.execute(sql)
#inserting
print ("Successfully Connected to mySQL instance (RDS)")

filename = "dummy.csv"
bucket = 'cloud6331-assign3'
conn = boto.connect_s3(access_key, secret_key)
rs = conn.get_all_buckets()
for b in rs:
    print (b)
nonexistent = conn.lookup(bucket)
if nonexistent is None:
    print ('Not there!')   
bucket = conn.get_bucket(bucket, validate=True)


#Read File from bucket
selected_file = bucket.get_key(filename)
fcontent =reader(StringIO(selected_file.read()),delimiter=',',quotechar = '"')
print ("File read successfully")
#render page to upload file

@app.route('/')
@app.route('/index')
def get_upload_page():
    print ("Calling get_upload_page method")
    return render_template("upload.html")


#Method for uploading

@app.route('/upload_file', methods= ['POST'])
def upload_file():
    file = request.files['file']
    filename = secure_filename(file.filename)
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    print(filename)
    key = bucket.new_key(filename)
    print (filename)
    start = time.time()
    key.set_contents_from_string(file.read())
    print ("file uploaded")
    end = time.time()
    taken_time = end - start
    key.set_acl('public-read')
    print ("File uploaded successfull \n time taken to upload file is : " + str(taken_time))
    return insert(filename)


#Retrievng page for dynamic query using city as the field
@app.route('/dynamic_query',methods = ['GET'])
def query():
    cursor = connection.cursor()
    sql = "Select DISTINCT country_or_territory from newdata"
    cursor.execute(sql)
    
    values = cursor.fetchall()
    array = []
    for value in values:
        print (str(value[0].encode('ascii', 'ignore')))
        array.append(str(value[0].encode('ascii', 'ignore')))
        #print array
    return render_template("dynamic_query.html", array = array)


    #Random Query execution 
@app.route("/run_query", methods = ["POST"])
def randomqueryexecution():
    select1 = request.form.get('CITY').encode('ascii', 'ignore')
    select2 = request.form.get('LorM').encode('ascii', 'ignore')
    cursor = connection.cursor()
    d = dict(enumerate(string.ascii_lowercase, 1))
    pos = string.lowercase.index(select2)
    columnname = ''
    if pos == 11:
        columnname = 'aug'
    else:
        columnname = 'sep'
    starttime = time.time()
    number = 50    
    sql = "Select * from newdata WHERE country_or_territory='"+select1+"' AND "+columnname+" <= '"+str(number)+"';"
    cursor.execute(sql)
    print('Executing Queries..')

    all_data = cursor.fetchall()
    stoptime = time.time()
    #Calculate total time for data insertion
    insert_time = stoptime - starttime
    print ("Time taken to insert: " + str(insert_time) + "sec") 
    city_result = []
    html = "<center><h2> Dynamic query result</h2></center>"
    html = "<h4> Data " + str('CITY') + " : <br></h4>"     
    html += "<table style = 'border: 1px solid black'><tr><th>country</th> <th>count</th></tr>"
    for data in all_data:
        #print data["CITY"]
       # if data["CITY"] == str(CITY):
            html += "<tr><td>" + data[1].encode('ascii', 'ignore') + "</td><td>"  + str(data[pos]) + "</td><tr>"     
    html += "</table>"
    return html
    


  
#Dynamo DB insertion in the following method
def insert(file_name):
     #Skip the header line
    table = 'newdata'
    #Calculate start time for data insertion
    starttime = time.time()
    
    mysqlcsv.main(file_name, user, password, host, table, database)       
    #Calculate end time for data insertion
    stoptime = time.time()
    #Calculate total time for data insertion
    insert_time = stoptime - starttime
    print ("Time taken to insert: " + str(insert_time) + "sec") 
    connection.commit()
    result = display(table)
    return result      
            
            
@app.route('/display')
def display(table):      
    cursor = connection.cursor()
    sql = "USE cloudDB"
    cursor.execute(sql)
    display_result = "Select * from "+table+" LIMIT 20;"
    cursor.execute(display_result)
    rows = cursor.fetchall()
    connection.commit()
    html = "<table><tr><th>FIRST NAME </th> <th> Last Name</th></tr>"
    for row in rows:
        firstname = str(row[1])
        lastname = str(row[0])
        html += "<tr><td>" + firstname + "</td><td>" + lastname + "</td><tr>" 
    html += "</table>"
    return  """
            <!doctype html>
            <form action = "/dynamic_query" method ="post" class='form-signin' enctype = "multipart/form-data">
                <input type = "submit" value="Query" />
            </form>
            <title>Uploaded</title>
            <h1>%s</h1>
            </html>
            """%html

port = os.getenv('VCAP_Aapp_PORT','6060')
if __name__ == '__main__':
    app.secret_key = 'world'
    app.run(host='127.0.0.1',port=int(port))