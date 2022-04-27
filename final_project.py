import json
import unittest
import mysql.connector
from mysql.connector import errorcode
import sys
from datetime import datetime

#Replace these with your credentials
username = "lingj"
password = "rose"

class DAO():

    #Replace database with AIS_PROJECT before submission
    def __init__( self, stub=False ):
        self.is_stub=stub
        self.connection = ''

        try:
            self.connection = mysql.connector.connect(host="localhost",user=username,password=password,database="aistestdata")

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Username or Password is incorrect")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        if not self.connection:
            sys.exit("Connection failed: exiting.")

    def deploy_database(self):
        deploy_statement = """
        DROP DATABASE IF EXISTS AIS_PROJECT;
        CREATE DATABASE AIS_PROJECT;
        USE AIS_PROJECT;

        DROP TABLE IF EXISTS AIS_MESSAGE;
        DROP TABLE IF EXISTS MAP_VIEW;
        DROP TABLE IF EXISTS PORT;
        DROP TABLE IF EXISTS POSITION_REPORT;
        DROP TABLE IF EXISTS STATIC_DATA;
        DROP TABLE IF EXISTS VESSEL;

        CREATE TABLE VESSEL(
            IMO MEDIUMINT UNISGNED NOT NULL,
            Flag VARCHAR(40),
            Name VARCHAR(40),
            Built SMALLINT,
            CallSIgn VARCHAR(8),
            Length SMALLINT,
            BREADTH SMALLINT,
            Tonnage MEDIUMINT,
            MMSI INT,
            Type VARCHAR(30),
            Status VARCHAR(40),
            Owner VARCHAR(80),
            PRIMARY KEY(IMO)
        );
        
        CREATE TABLE AIS_MESSAGE(
            ID MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
            Timestamp DATETIME,
            MMSI INT,
            Class ENUM('Class A','Class B','AtoN','Base Station','SAR Airborne','Search and Rescue Transponder','Man Overboard Device'),
            Vessel_IMO MEDIUMINT UNSIGNED,
            PRIMARY KEY(ID),
            FORIEGN KEY(Vessel_IMO) REFERENCES VESSEL(IMO)
        );

        CREATE TABLE MAP_VIEW(
            ID MEDIUMINT NOT NULL,
            Name VARCHAR(10),
            LongitudeW DECIMAL(9,6),
            LatitudeS DECIMAL(8,6),
            LongitudeE DECIMAL(9,6),
            LatitudeN DECIMAL(8,6),
            Scale ENUM('1','2','3'),
            RasterFile VARCHAR(100),
            ImageWidth SMALLINT,
            ImageHeight SMALLINT,
            ActualLongitudeW DECIMAL(9,6),
            ActualLatitudeS DECIMAL(8,6),
            ActualLongitudeE DECIMAL(9,6),
            ActualLatitudeN DECIMAL(8,6),
            ContainerMapView_ID MEDIUMINT,
            PRIMARY KEY(ID),
            FOREIGN KEY(ContainerMapView_ID) REFERENCES MAP_VIEW(id)
        );

        CREATE TABLE PORT(
            ID SMALLINT NOT NULL,
            LoCode CHAR(5),
            Name VARCHAR(30),
            Country VARCHAR(80),
            Longitude DECIMAL(9,6),
            Latitude DECIMAL(8,6),
            Wesbsite VARCHAR(120),
            MapView1_ID MEDIUMINT,
            MapView2_ID MEDIUMINT,
            MapView3_ID MEDIUMINT,
            PRIMARY KEY(ID),
            FORIEGN KEY(MapView1_ID) REFERENCES MAP_VIEW(ID),
            FORIEGN KEY(MapView2_ID) REFERENCES MAP_VIEW(ID),
            FORIEGN KEY(MapView3_ID) REFERENCES MAP_VIEW(ID)
        );

        CREATE TABLE STATIC_DATA(
            AISMessage_ID MEDIUMINT UNSIGNED NOT NULL,
            AISIMO INT,
            CallSign VARCHAR(8),
            Name VARCHAR(30),
            VesselType VARCHAR(30),
            CargoType VARCHAR(30),
            Length SMALLINT,
            Breadth SMALLINT,
            Draught SMALLINT,
            AISDestination VARCHAR(50),
            ETA DATETIME,
            DestinationPort_ID SMALLINT,
            PRIMARY KEY(AISMessage_ID),
            FORIEGN KEY(AISMessage_ID) REFERENCES AIS_MESSAGE(ID),
            FOREIGN KEY(DestinationPort_ID) REFERENCES PORT(ID)
        );
        
        CREATE TABLE POSITION_REPORT(
            AISMessage_ID MEDIUMINT UNSIGNED NOT NULL,
            NavigationalStatus VARCHAR(40),
            Longitude DECIMAL(9,6),
            Latitude DECIMAL(8,6),
            RoT DECIMAL(4,1),
            SoG DECIMAL(4,1),
            CoG DECIMAL(4,1),
            Heading SMALLINT,
            LastStaticData_ID MEDIUMINT UNSIGNED,
            MapView1_ID MEDIUMINT,
            MapView2_ID MEDIUMINT,
            MapView3_ID MEDIUMINT,
            PRIMARY KEY(AISMessage_ID),
            FORIEGN KEY(AISMessage_ID) REFERENCES AIS_MESSAGE(ID),
            FOREIGN KEY(LastStaticData_ID) REFERENCES STATIC_DATA(ID),
            FORIEGN KEY(MapView1_ID) REFERENCES MAP_VIEW(ID),
            FORIEGN KEY(MapView2_ID) REFERENCES MAP_VIEW(ID),
            FORIEGN KEY(MapView3_ID) REFERENCES MAP_VIEW(ID)
        );
        """
        self.run(deploy_statement)

    def run(self, query):
        """
        Run any query
        :param query: an SQL query
        :type query: str
        :return: the result set as Python list of tuples
        :rtype: list
        """
        mycursor = self.connection.cursor()
        mycursor.execute(query)
        result = mycursor.fetchall()
        mycursor.close()
        
        return result


    def insert_message_batch(self, batch):
        """
        Insert a batch of messages

        :param batch: an array of AIS messages (objects)
        :type batch: list 
        :return: Number of successful insertions
        :rtype: int
        """
        
        if type(batch) is str:
            print("Incorrect parameter type: should be a list of messages")
            return -1

        if self.is_stub:
            return len(batch)
        
        inserted = 0
        
        for message in batch:

            if message['MsgType'] == 'position_report':
                report = PositionReport(message)

            if message['MsgType'] == 'static_data':
                report = StaticData(message)

            try:
                query = "insert into AIS_MESSAGE values {};".format( report.to_shared_sql_values() )
                #print(query)
                self.run(query)

                query = "insert into POSITION_REPORT VALUES {};".format( report.to_position_report_sql_values() )
                #print(query)
                self.run(query)

                query = "insert into VESSEL VALUES {};".format(report.to_vessel_sql_values())
                #print(query)
                self.run(query)

                inserted+=1

            except Exception as e:
                print(e)

        if len(batch) and inserted != 0:
            return True

        return inserted

    def delete_old_ais_messages(self): #How to test???
        """
        Deletes all ais messages in the database more than 5 minutes older than current time

        :return: Number of deletions
        :rtype: int
        """

        current_timestamp = datetime.timestamp(datetime.now())
        #print(current_timestamp)

        deleted = 0
        
        query = "select timestamp from AIS_MESSAGE;"
        list_of_timestamps = self.run(query)

        for item in list_of_timestamps:
            if (item - current_timestamp) > 5:
                query = "delete from AIS_MESSAGE where timestamp={};".format(item)
                self.run(query)
                deleted+=1
        
        return deleted

    def read_recent_position_given_MMSI(self, MMSI):
        """
        Reads all of the recent positions of ships given a specific MMSI

        :param MMSI: The know MMSI of a given vessel
        :MMSI type: int
        :return: An array containing all results
        :rtype: array
        """
        length_check = str(MMSI)
        if len(length_check) != 9:
            print("Error: MMSI must be a 9 digit number. Returning empty string...")
            return ""
        
        query = """
        select max(timestamp), MMSI, latitude, vessel_IMO 
        from ais_message, position_report 
        where position_report.aismessage_id=ais_message.id and MMSI={};""".format(MMSI)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        #print(results)
        return results

    #This will not have the limit of 100 once we use the new database. The test database has too many records to not have the limit
    #Remove "limit 100" before submitting!

    #query for MMSIs and Timestamps, then use sub-query ("With messages as(sub-query)")
    def read_all_recent_ship_positions(self):
        """
        Reads all most recent positions of every ship in the database

        :return: an array containing all results
        :rtype: array
        """
        query = """
        select MMSI, latitude, longitude, Vessel_IMO 
        from ais_message, position_report 
        where position_report.AISMessage_Id=ais_message.id order by timestamp limit 100;"""

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    #Help? What does this query look like? My attempts give me an empty set
    #Attempt: select MMSI, latitude, longitude, AISIMO, name from ais_message, position_report, static_data 
    # where ais_message.id=position_report.AISMessage_Id and position_report.AISMessage_id=static_data.AISMessage_Id limit 10;
    #results in an empty set?
    def read_vessel_info(self, MMSI, IMO='', name=''):
        query = """
        select MMSI, latitude, longitude, AISIMO, name 
        from ais_message, position_report, static_data 
        where ais_message.id=position_report.AISMessage_Id
        limit 10;"""

        document= self.run(query)
        results = [tuple(str(item) for item in t) for t in document]

        return results

    def read_recent_positions_given_tile(self, tile_id):
        pass

    def read_all_ports_matching_name(self, port_name='', country=''):
        """
        Reads every port that matches a given name or country

        :param port_name: the specific name of the port that is being searched for
        :port_name type: str
        :param country: the specific country that contains the ports
        :country type: str
        :return: an array containing all results
        :rtype: array
        """
        if port_name=='' and country == '':
            print("Error: Cannot lookup port with no information given. Returning empty string...")
            return ""

        query = "select ID, Name, Country, Latitude, Longitude from port where name='{}' or country='{}' limit 100;".format(port_name,country)

        document= self.run(query)
        results = [tuple(str(item) for item in t) for t in document]

        return results

    def read_recent_positions_given_tile_and_port(self, port_name, country=''):
        if port_name == '' and country != '':
            query = """select distinct port.name, MMSI, rpt.latitude, rpt.longitude, msg.Vessel_IMO, scale 
            from ais_message as msg, position_report as rpt, map_view as map, port 
            where msg.id=rpt.aisMessage_id and scale=3 and port.country='{}' limit 10;""". format(port_name)

        if port_name != '' and country == '':
            query = """select distinct port.name, MMSI, rpt.latitude, rpt.longitude, msg.Vessel_IMO, scale 
            from ais_message as msg, position_report as rpt, map_view as map, port 
            where msg.id=rpt.aisMessage_id and scale=3 and port.name='{}' limit 100;""". format(port_name)

    def read_last_five_positions_given_MMSI(self, MMSI):
        """
        Reads the five most recent positions of a vessel given its MMSI

        :param MMSI: The given MMSI of a vessel
        :MMSI type: int
        :return: an array containing all results
        :rtype: array
        """
        length_check = str(MMSI)
        if len(length_check) != 9:
            print("Error: MMSI must be a 9 digit number. Returning empty string...")
            return ""
        
        query = """
        select timestamp, MMSI, latitude, longitude, Vessel_IMO 
        from ais_message, position_report 
        where position_report.AISMessage_Id=ais_message.id and MMSI={} order by timestamp desc limit 5;""".format(MMSI)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        #print(results)
        return results

    def read_recent_ship_positions_headed_to_port_ID(self, port_id):
        pass

    def read_recent_ship_positions_headed_to_port(self, port_name, country=''):
        pass

    def lookup_contained_tiles(self, tile_id):
        pass

    def get_tile_PNG(self, tile_id): 
        pass

            



#These classes extract data from a json file and format it to be inserted into a database.
class Message:

    def __init__(self, msg ):
        
        self.timestamp = msg['Timestamp'][:-1].replace('T',' ')
        self.mmsi = msg['MMSI']
        self.equiptclass = msg['Class']

    def to_shared_sql_values( self ):
        #first "NULL" value is the ID field, which cannot be null. This will change once we use this without the "aistestdata" database
        #Once using the new database, remove the first NULL value
        return "(NULL, '{}', {}, '{}', NULL)".format( self.timestamp, self.mmsi, self.equiptclass )


class PositionReport( Message ):

    def __init__(self, msg):

        super().__init__(msg)

        self.id = ''
        self.status = msg['Status']
        self.longitude = msg['Position']['coordinates'][1]
        self.latitude = msg['Position']['coordinates'][0]
        self.rot = msg['RoT'] if 'RoT' in msg else 'NULL'
        self.sog = msg['SoG'] if 'SoG' in msg else 'NULL'
        self.cog = msg['CoG'] if 'CoG' in msg else 'NULL'
        self.heading = msg['Heading'] if 'Heading' in msg else 'NULL'
        
    def to_position_report_sql_values( self ):
        
        if not self.id: # without a valid key, no output
            return ''
        return f"({self.id}, '{self.status}', {self.longitude}, {self.latitude}, {self.rot}, {self.sog}, {self.cog}, {self.heading},NULL,NULL,NULL,NULL)"

class StaticData( Message ):

    def __init__(self, msg):

        super().__init__(msg)

        self.IMO = msg['IMO']
        self.name = msg['Name']
        self.vessel_type = msg['VesselType']
        self.length = msg['Length']
        self.breadth = msg['Breadth']

    def to_vessel_sql_values( self ):
        return "('{}', NULL,'{}', NULL, NULL,'{}','{}', NULL, NULL,'{}', NULL, NULL)".format(self.IMO, self.name, self.length, self.breadth, self.vessel_type)


class TMB_test(unittest.TestCase):
    
    multi_batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290},
                {\"Timestamp\":\"2020-11-18T00:06:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    single_message = "[{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}]"

    def test_insert_message_batch_interface_1(self):
        """
        Function `insert_message_batch` takes an array of messages as an input.
        """
        tmb = DAO(True)
        array = json.loads( self.multi_batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 

    def test_insert_message_batch_interface_2(self):
        """
        Function `insert_message_batch` fails nicely if input is still a string
        """ 
        tmb = DAO(True)
        inserted_count = tmb.insert_message_batch( self.multi_batch )
        self.assertEqual( inserted_count, -1) 

    def test_insert_message_batch_integration(self):
        """
        Function `insert_message_batch` inserts messages in the MySQL table
        """
        tmb = DAO()
        array = json.loads( self.multi_batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 

    
    def insert_single_message_return_test(self):
        """
        Function `insert_message_batch` returns True if a single message is inserted
        """
        tmb = DAO(True)
        result = tmb.insert_message_batch(self.single_message)
        self.assertTrue(result)
    
    
    def test_read_recent_position_given_MMSI_interface_fail(self):
        """
        Function `read_recent_position_given_MMSI` fails nicely if no parameter is passed
        """
        MMSI = 1000
        tmb = DAO(True)
        value = tmb.read_recent_position_given_MMSI(MMSI)
        self.assertTrue(value=="")
    
    def test_read_recent_position_given_MMSI_integration(self):
        """
        Function `read_recent_position_given_MMSI` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_recent_position_given_MMSI(219007155)
        self.assertEqual(result, [('2020-11-18 02:38:20', '219007155', '54.947323', 'None')])


    def test_read_all_recent_ship_positions_integration(self):
        """
        Function `read_all_recent_ship_positions` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_all_recent_ship_positions()
        self.assertEqual(result[2], ('265866000', '54.763183', '12.415067', '9217242'))

    def test_read_last_five_positions_given_MMSI_integration(self):
        """
        Function `read_last_five_positions_given_MMSI` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_last_five_positions_given_MMSI(219007155)
        self.assertEqual(result[0], ('2020-11-18 02:38:20', '219007155', '54.947338', '11.107798', 'None') )

    def test_read_all_ports_matching_name_interface(self):
        """
        Function `read_all_ports_matching_name` fails nicely if no parameters are passed
        """
        tmb = DAO()
        result = tmb.read_all_ports_matching_name()
        self.assertEqual(result, "")

    def test_read_all_ports_matching_name_integration(self):
        """
        Function `read_all_ports_matching_name` returns the result of the query as an array
        """
        tmb = DAO()
        result = tmb.read_all_ports_matching_name(port_name='Ensted')
        self.assertEqual(result, [('4378', 'Ensted', 'Denmark', '55.022778', '9.439167')])

    def test_read_vessel_info_integration(self):
        """
        Function 'read_vessel_info' returns the result of the query as an array
        """
        tmb = DAO()
        result = tmb.read_vessel_info()
        self.assertEqual(result, [()])
"""
    def test_delete_old_message(self):
        pass

    def test_read_recent_positions_given_tile(self):
        pass

    def test_read_recent_positions_given_tile_and_port(self):
        pass

    def test_read_recent_ship_positions_headed_to_port_ID(self):
        pass

    def test_read_recent_ship_positions_headed_to_port(self):
        pass

    def test_lookup_contained_tiles(self):
        pass

    def test_get_tile_PNG(self):
        pass
"""
    


if __name__ == '__main__':
    unittest.main()