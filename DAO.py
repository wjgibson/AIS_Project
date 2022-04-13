import unittest
import mysql.connector
from mysql.connector import errorcode

import sys
import operator
from datetime import *
import json

## Modify to fit your configuration
USER='wjgib'
PASSWORD='Oliver'

class SQL_runner():
    """
    A SQL connector
    """

    def __init__(self, user, pw, host='127.0.0.1', db=''):

        self.cnx = None

        try:
            print("Connecting to database ", db, "... ", end='')
            self.cnx = mysql.connector.connect( user=user, password=pw, database=db, host=host)
            print("OK")
    
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        if not self.cnx:
            sys.exit("Connection failed: exiting.")

    def __del__(self):
        if self.cnx is not None:
            print("Closing database connection")
            self.cnx.close()

    
    def run(self, query ):
        """ Run a query
        :param query: an SQL query
        :type query: str
        :return: the result set as Python list of tuples
        :rtype: list
        """
        cursor = self.cnx.cursor()
        cursor.execute(  query )
        result = cursor.fetchall()
        cursor.close()

        return result

class DAO:

    def insert_message_batch( self, batch ):
        """
        Insert a batch of messages

        :param batch: an array of AIS messages (objects)
        :type batch: list 
        :return: Number of successful insertions
        :rtype: int
        """
        if type( batch ) is str:
            print("Incorrect parameter type: should be a list of messages")
            return -1
        if self.is_stub:
            return len(batch)

        #WHY IS THIS WRONG?
        cursor=SQL_runner ( USER,PASSWORD, 127.0.0.1, db='aistestdata')

        inserted = 0
        
        for msg in batch:
            
            if msg['MsgType']=='position_report':
                pr = PositionReport( msg )

            try:
                query = "insert into AIS_MESSAGE values {}".format( pr.to_shared_sql_values() )
                print(query)
                cursor.execute(query)
                
                pr.id = cursor.lastrowid

                query = "insert into POSITION_REPORT VALUES {}".format( pr.to_position_report_sql_values() )
                print(query)
                cursor.execute(query)
                print(f"INSERTED: {cursor.rowcount}")

                con.commit()
                inserted += cursor.rowcount
            except Exception as e:
                print(e)
            
        return inserted

class Message:

    def __init__(self, msg ):
        
        self.timestamp = msg['Timestamp'][:-1].replace('T',' ')
        self.mmsi = msg['MMSI']
        self.equiptclass = msg['Class']

    def to_shared_sql_values( self ):
        return "(NULL, '{}', {}, '{}', NULL)".format( self.timestamp, self.mmsi, self.equiptclass )


class PositionReport( Message ):

    def __init__(self, msg):

        super().__init__(msg)

        self.id = None
        self.status = msg['Status']
        self.longitude = msg['Position']['coordinates'][1]
        self.latitude = msg['Position']['coordinates'][0]
        self.rot = msg['RoT'] if 'RoT' in msg else 'NULL'
        self.sog = msg['SoG'] if 'SoG' in msg else 'NULL'
        self.cog = msg['CoG'] if 'CoG' in msg else 'NULL'
        self.heading = msg['Heading'] if 'Heading' in msg else 'NULL'
        
    def to_position_report_sql_values( self ):
        
        if not self.id: # without a valid key, no output
            return None
        return f"({self.id}, '{self.status}', {self.longitude}, {self.latitude}, {self.rot}, {self.sog}, {self.cog}, {self.heading},NULL,NULL,NULL,NULL)"