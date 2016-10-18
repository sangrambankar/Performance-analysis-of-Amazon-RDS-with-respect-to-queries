'''
Created on Jun 13, 2016

@author: Sangram
'''
import boto
import sys
from boto.s3.key import Key
import boto.s3.connection
import time
import mysql
import random
from _csv import reader
from StringIO import StringIO
import mysql.connector

import memcache
memc = memcache.    Client(['127.0.0.1:11211'], debug=1)


#uploading to the bucket
access_key = ''
secret_key = ''


#connecting to rds instance

credentials= {'host': '',
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

###inserting data into the MySqlDB
def CSVload(fcontent,cursor):
       
    #Skip the header line
    fcontent.next()
    #Calculate start time for data insertion
    starttime = time.time()
    counter = 0
    for row in fcontent:
        for i in range(0,4):
            if row[i] == "":
                row[i] = "0"    
        #place_name = row[4].replace("'","")  
        insert = "INSERT INTO dummy1(id,first_name,last_name,mark,percent) VALUES(\"" + row[0]+ "\", \"" + row[1] + "\",\"" + row[2] + "\",\"" + row[3]+ "\",\"" + row[4] +"\")";
        r = cursor.execute(insert)
        counter += 1

    #Calculate end time for data insertion
    stoptime = time.time()
    #Calculate total time for data insertion
   insert_time = stoptime - starttime
    print ("Time taken to insert: " + str(insert_time) + "sec")   

result = CSVload(fcontent,cursor)
connection.commit()
print ("Insertion to the Database done!!!")

def bucketinsert():
    print ("uploading file")
    k = Key(bucket)
    k.key = 'dummy1.csv'
    start=time.time()
    k.set_contents_from_filename('dummy1.csv')
    print ('uploaded the file')
    stop=time.time()
    exectime=stop-start
    print ("Execution time for uploading: " +str(exectime) +"sec")


def randomQueryGeneration(total):
    cursor = connection.cursor()
    sql = "USE cloudDB"
    cursor.execute(sql)
    print('Executing Queries..')
    range_value = range(1,total)
    start = time.time()
    for count in range_value:
        #id = random.randint(1000, 300000)
        id=4000
        query = "Select iddata from data where iddata = " + str(id+count)
        cursor.execute(query)
        #print "query no : " + str(count)
    end = time.time()
    taken_time = end - start
    print ("time taken for "+str(total)+" queries without memcache : " + str(taken_time) + " Seconds.")


#RandomQueryGeneration with memcache
def memchacheQuerygen(total):
    listarray = []
    start = time.time()
    for i in range(0,total):
        id1 = 4000
        #id = random.randint(1000, 300000)
        cached = memc.get(str(id1))
        if not cached:
            #print "without memcache"
            query = "Select iddata from data where iddata = " + str(id1)
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                listarray.append(str(row[0]))
            valueHere = memc.set(str(id1),listarray,150)
            
            #print valueHere
            id1 = id1 +1
        
        else:
            print ("with memcache")
            for row in cached:
                print (row)
    end = time.time()
    taken_time = end - start
    print ("time taken (USING MEMCACHE) for "+str(total)+" queries : " + str(taken_time) + " Seconds.")
    
#result2=memcache(1000)


# for step 6. random queries to generate values of 200 to 800 touples. number_execution is the number of queries going to genrerate
def randomlargetuple(number):

    cursor = connection.cursor()
    sql = "USE cloudDB"
    cursor.execute(sql)
    print ("Executing Queries")
                
    #query 
    #it will generate random queries 
    range_value = range(1,number)
                    
    start = time.time()
    for count in range_value:
        id = random.triangular(1000, 300000, 150000) #generates random key value to use for query
        query = "Select iddata from data where id = " + str(id)+" LIMIT 500"
        cursor.execute(query)
        print ("qeury no : " + str(count))
    end = time.time()
    taken_time = end - start
    print ("time taken for "+str(number)+" queries : " + str(taken_time) + " Seconds.")
    
    

def main(argv):
    i = True
    while i != False:
        option = int(raw_input('1)Upload file in the bucket\n2)Random Query Generation\n 3)200-300 tuples\n'))
        if option == 1:
            bucketinsert()

        if option == 2:
            total = int(raw_input("No of Queries you want to generate? "))
            randomQueryGeneration(total)
            memchacheQuerygen(total)
        if option == 3:
            total = int(raw_input("No of Queries you want to generate? "))
            randomlargetuple(total)
        i = False

if __name__ == '__main__':
    main(sys.argv)
    







