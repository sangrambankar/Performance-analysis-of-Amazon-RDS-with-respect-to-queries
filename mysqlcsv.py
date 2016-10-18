'''
Created on Jun 14, 2016

@author: dvya2
'''
import os
import re
import sys
import csv
import time
import argparse
import collections
import mysql
import warnings 
import mysql.connector


def get_type(s):
    """Find type for this string
    """
    number_formats = (
        (int, 'integer'),
        (float, 'double'),
    )
    for cast, number_type in number_formats:
        try:
            cast(s)
        except ValueError:
            pass
        else:
            return number_type

    # check for timestamp
    dt_formats = (
        ('%Y-%m-%d %H:%M:%S', 'datetime'),
        ('%Y-%m-%d %H:%M:%S.%f', 'datetime'),
        ('%Y-%m-%d', 'date'),
        ('%H:%M:%S', 'time'),
    )
    for dt_format, dt_type in dt_formats:
        try:
            time.strptime(s, dt_format)
        except ValueError:
            pass
        else:
            return dt_type
   
    # doesn't match any other types so assume text
    if len(s) > 255:
        return 'text'
    else:
        return 'varchar(255)'


def most_common(l):
    """Return most common value from list
    """
    return max(l, key=l.count)

# in get_col_types I have changed max_rows to 1 so I can control column type def with faked data row
# that contains properly-formatted values
# potential todo: add functionality to choose between majority type (current functionality) or 
# default to varchar if ever a wild string appears
def get_col_types(input_file, max_rows=1): 
    """Find the type for each CSV column
    """
    csv_types = collections.defaultdict(list)
    reader = csv.reader(open(input_file))
    # test the first few rows for their data types
    for row_i, row in enumerate(reader):
        if row_i == 0:
            header = row
        else:
            for col_i, s in enumerate(row):
                data_type = get_type(s)
                csv_types[header[col_i]].append(data_type)
 
        if row_i == max_rows:
            break

    # take the most common data type for each row
    return [most_common(csv_types[col]) for col in header]


def get_schema(table, header, col_types):
    """Generate the schema for this table from given types and columns
    """
    schema_sql = """CREATE TABLE IF NOT EXISTS %s ( 
        id int NOT NULL AUTO_INCREMENT,""" % table 

    for col_name, col_type in zip(header, col_types):
        schema_sql += '\n%s %s,' % (col_name, col_type)

    schema_sql += """\nPRIMARY KEY (id)
        ) DEFAULT CHARSET=utf8;"""
    return schema_sql


def get_insert(table, header):
    """Generate the SQL for inserting rows
    """
    field_names = ', '.join(header)
    field_markers = ', '.join('%s' for col in header)
    return 'INSERT INTO %s (%s) VALUES (%s);' % \
        (table, field_names, field_markers)


def safe_col(s):
    return re.sub('\W+', '_', s.lower()).strip('_')


def main(input_file, user, password, host, table, database):
    insert_sql= ''
    print "Importing `%s' into MySQL database `%s.%s'" % (input_file, database, table)
    db = mysql.connector.connect(user=user, password=password,host=host,database=database)
    cursor = db.cursor()
    # create database and if doesn't exist
    # define table
    print 'Analyzing column types ...'
    col_types = get_col_types(input_file)
    print col_types

    header = None
    for row in csv.reader(open(input_file)):
        if header == None:
            header = [safe_col(col) for col in row]
            schema_sql = get_schema(table, header, col_types)
            print(schema_sql)
            # create table
            cursor.execute('DROP TABLE IF EXISTS %s;' % table)
            cursor.execute(schema_sql)
            # SQL string for inserting data
            
    sql="LOAD DATA LOCAL INFILE '" + input_file + "' INTO TABLE " + table + " FIELDS TERMINATED BY ','  ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES"
    cursor.execute(sql)
    
    # commit rows to database
    print 'Committing rows to database ...'
    db.commit()
    print 'Done!'

