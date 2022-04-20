from os import curdir
import mysql.connector
from mysql.connector import errorcode

try:
    db = mysql.connector.connect(
    host="localhost",
    user="wjgib",
    password="Oliver",
    database="aistestdata"
    )

    mycursor = db.cursor()
    mycursor.execute("create database test")
    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Username or Password is incorrect")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)