import mysql.connector
from mysql.connector import errorcode

class DAO():

    def __init__( self, stub=False ):
        self.is_stub=stub
    
    try:
        db = mysql.connector.connect(
        host="localhost",
        user="wjgib",
        password="Oliver",
        database="aistestdata"
        )

    
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Username or Password is incorrect")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    def run(self):
        mycursor = self.db.cursor()
        mycursor.execute("create database test")

    def insert_message_batch(self):
        