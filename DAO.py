from sqlite3 import Cursor
import unittest
import mysql.connector
from mysql.connector import errorcode

import sys
from datetime import *

import json


## Modify to fit your configuration
USER='wjgib'
PASSWORD='Oliver'

#file = open('sample_input.json')
#batch = json.load(file)

class SQL_runner():

    def __init__(self, user, pw, host='127.0.0.1', db=''):

        self.cnx = None

        try:
            print("Connecting to database ", db, "... ", end='')
            self.cnx = mysql.connector.connect( user=user, password=pw, database=db, host=host)
            print("OK")
    
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Username or Password is incorrect")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        if not self.cnx:
            sys.exit("Connection failed...exiting...")

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

        


class DAO():
    
    def __init__( self, stub=False ):
        self.is_stub=stub

    cnx = SQL_runner( USER, PASSWORD, db='aistestdata' )

    def deploy_database(self):
        deployment="""
        drop database if exists aisproject;
        create databse aisproject;
        use aisproject;
        """
        self.cnx.run("drop database if exists aisproject;create databse aisproject;use aisproject;")
        

    def insert_message_batch( self, batch ): #Testing complete
        if type( batch ) is str:
            print("Incorrect parameter type: should be a list of messages")
            return -1
        if self.is_stub:
            return len( batch )
        
        inserted = 0
    

        for msg in batch:

            if msg[ 'MsgType' ] == 'position_report':
                pr = PositionReport( msg )

            try:
                query = "insert into AIS_MESSAGE values {};".format( pr.to_shared_sql_values() )
                #print(query)
                self.cursor.run(query)
                
                pr.id = self.cursor.lastrowid

                query = "insert into POSITION_REPORT VALUES {};".format( pr.to_position_report_sql_values() )
                #print(query)
                self.cursor.run(query)
                #print(f"INSERTED: {cursor.rowcount}")

                inserted += self.cursor.rowcount
            except Exception as e:
                print(e)
                
        return inserted

    def delete_old_data(self):
        pass

    def MMSI_position_lookup(self, MMSI):
        try:
            query="select Timestamp from AIS_MESSAGE where MMSI={} limit 1;".format(MMSI)
            print(query)
            cursor = SQL_runner
            cursor.run(query)
            result = cursor.fetchall()
            return result
        except Exception as e:
            print(e)
            

class Message:

    def __init__(self, msg ):
        
        self.timestamp = msg['Timestamp'][:-1].replace('T',' ')
        self.mmsi = msg['MMSI']
        self.equiptclass = msg['Class']

    def to_shared_sql_values( self ):
        return "(NULL, '{}', {}, '{}', NULL)".format( self.timestamp, self.mmsi, self.equiptclass )


class PositionReport( Message ): #Testing Complete

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

        

class TMB_test(unittest.TestCase):
    
    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    def test_insert_message_batch_interface_1( self  ):
        """
        Function `insert_message_batch` takes an array of messages as an input.
        """
        tmb = DAO(True) 
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 

    def test_insert_message_batch_interface_2( self  ):
        """
        Function `insert_message_batch` fails nicely if input is still a string
        """
        tmb = DAO(True) 
        inserted_count = tmb.insert_message_batch( self.batch )
        self.assertEqual( inserted_count, -1) 


    def test_insert_message_batch( self  ):
        """
        Function `insert_message_batch` inserts messages in the MySQL table
        """
        tmb = DAO()
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 


    def test_position_report_creation( self ):

        pr = {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}
        
        tmb = DAO(True)
        pr = PositionReport( pr )  
        self.assertEqual( pr.timestamp, "2020-11-18 00:00:00.000")

    def delete_old_records(self):
        pass

    def test_MMSI_lookup(self):
        tmb = DAO()
        timestamp=tmb.MMSI_position_lookup(219007155)
        self.assertEqual(timestamp,"2020-11-18 00:00:00")

if __name__ == '__main__':
    db = DAO()
    db.deploy_database
    unittest.main()
