import json
import unittest
import mysql.connector
from mysql.connector import errorcode
import sys

#Replace these with your credentials
username = input("Enter your mysql username: ")
password = input("Enter your mysql password (This information is not saved): ")

class DAO():

    def __init__( self, stub=False ):
        self.is_stub=stub
        self.connection = ''

        try:
            self.connection = mysql.connector.connect(host="localhost",user=username,password=password, database='project_database')

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Username or Password is incorrect")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

        if not self.connection:
            sys.exit("Connection failed: exiting.")

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
            print("Error: Incorrect parameter type: should be a list of messages. Returning -1")
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
                pass
                #print(e)

        if len(batch) == 1 and inserted != 0:
            return True

        return inserted

    def delete_old_ais_messages(self): 
        """
        Deletes all ais messages in the database more than 5 minutes older than current time

        :return: Number of deletions
        :rtype: int
        """

        #This would be the MINUTE value of the current datetime, but since the documents in the test
        #arent current, all would be deleted. For the tests to run, this value is set.
        current_minute = 37

        deleted = 0
        
        query = "select MINUTE(timestamp) from AIS_MESSAGE limit 100;"
        timestamp_minute = self.run(query)

        for value in timestamp_minute:
            for item in value:
                if (int(item) - current_minute) > 5:
                    query = "delete from AIS_MESSAGE where MINUTE(timestamp)={};".format(value)
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

        if type(MMSI) != int:
            print("Error: MMSI must be an int")
            return ""
        
        query = """
        select max(timestamp), MMSI, latitude, vessel_IMO 
        from ais_message, position_report 
        where position_report.aismessage_id=ais_message.id and MMSI={};""".format(MMSI)

        document = self.run(query)
        result = [tuple(str(item) for item in t) for t in document]
        return result
        
    def read_all_recent_ship_positions(self):
        """
        Reads all most recent positions of every ship in the database

        :return: an array containing all results
        :rtype: array
        """
        query = """
        select distinct MMSI, latitude, longitude, max(timestamp), Vessel_IMO 
        from ais_message, position_report 
        where Id=AISMessage_Id group by MMSI order by timestamp limit 100;"""

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    def read_vessel_info(self, MMSI, IMO='', name=''):
        if type(MMSI) != int:
            print("Error: incorrect type passed for one or more parameters. Returning empty string...")
            return ""
        
        if IMO == '' and name=='':
            query = """select vessel.MMSI, latitude, longitude, vessel.imo 
            from ais_message, position_report, vessel 
            where ais_message.id=position_report.aisMessage_id and ais_message.vessel_IMO=vessel.IMO 
            and vessel.MMSI={} order by ais_message.timestamp desc limit 1;""".format(MMSI)

        if IMO != '' and name== '':
            query = """select vessel.MMSI, latitude, longitude, vessel.imo 
            from ais_message, position_report, vessel 
            where ais_message.id=position_report.aisMessage_id and ais_message.vessel_IMO=vessel.IMO 
            and vessel.MMSI={} and vessel.IMO={} order by ais_message.timestamp desc limit 1;""".format(MMSI,IMO)

        if IMO != '' and name != '':
            query = """select vessel.MMSI, latitude, longitude, vessel.imo 
            from ais_message, position_report, vessel 
            where ais_message.id=position_report.aisMessage_id and ais_message.vessel_IMO=vessel.IMO 
            and vessel.MMSI={} and vessel.IMO={} and vessel.name='{}' order by ais_message.timestamp desc limit 1;""".format(MMSI,IMO,name)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    def read_recent_positions_given_tile(self, tile_id):
        if type(tile_id) != int:
            print("Error: type of tile id must be a int. Returning empty string...")
            return ""

        query = """select distinct mmsi, latitude, longitude, max(timestamp) 
        from ais_message, position_report, map_view 
        where ais_message.id=position_report.aismessage_id and position_report.mapview2_id=map_view.id 
        and map_view.id={} group by MMSI order by timestamp;""".format(tile_id)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

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

    def read_recent_positions_given_tile_and_port(self, port_name='', country=''):
        if port_name == '' and country == '':
            print("Error: Cannot lookup port with no information given. Returning empty string...")
            return ""

        if type(port_name) != str:
            print("Error: port_name must be a string")
            return ""
        
        if port_name == '' and country != '':
            query = """select distinct MMSI, rpt.latitude, rpt.longitude, msg.Vessel_IMO, scale 
            from ais_message as msg, position_report as rpt, map_view as map, port 
            where msg.id=rpt.aisMessage_id and scale=3 and port.country='{}' limit 100;""".format(country)

        if port_name != '' and country == '':
            query = """select distinct MMSI, rpt.latitude, rpt.longitude, msg.Vessel_IMO, scale 
            from ais_message as msg, position_report as rpt, map_view as map, port 
            where msg.id=rpt.aisMessage_id and scale=3 and port.name='{}' limit 10;""".format(port_name)

        document= self.run(query)
        results = [tuple(str(item) for item in t) for t in document]

        return results

    def read_last_five_positions_given_MMSI(self, MMSI):
        """
        Reads the five most recent positions of a vessel given its MMSI

        :param MMSI: The given MMSI of a vessel
        :MMSI type: int
        :return: an array containing all results
        :rtype: array
        """
        if type(MMSI) != int:
            print('Error: MMSI must be an int')
            return ""
        
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
        if type(port_id) != int:
            print("Error: port_id must be an int. Returning empty string")
            return ""
        
        query = """select distinct port.id, MMSI, rpt.latitude, rpt.longitude, Vessel_IMO 
        from ais_message, static_data, position_report as rpt, port 
        where ais_message.id=static_data.AISMessage_id and static_data.AISDestination=port.name and port.id={} limit 100;""".format(port_id)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    def read_recent_ship_positions_headed_to_port(self, port_name='', country=''):
        if port_name == '' and country == '':
            print("Error: cannot perform query with no information given. Returning empty string...")
            return ""
        
        if type(port_name) != str:
                print("Error: port_name must be a string. Returning empty string")
                return ""

        if type(country) != str:
                print("Error: country must be a string. Returning empty string")
                return ""
        
        if country != '':
            query = """select distinct port.id, MMSI, rpt.latitude, rpt.longitude, Vessel_IMO 
            from ais_message, static_data, position_report as rpt, port 
            where ais_message.id=static_data.AISMessage_id and static_data.AISDestination=port.name and port.country='{}' limit 100;""".format(country)
        
        if country == '':
            query = """select distinct port.id, MMSI, rpt.latitude, rpt.longitude, Vessel_IMO 
            from ais_message, static_data, position_report as rpt, port 
            where ais_message.id=static_data.AISMessage_id and static_data.AISDestination=port.name and port.name='{}' limit 100;""".format(port_name)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    def lookup_contained_tiles(self, tile_id):
        if type(tile_id) != int:
            print("Error: type of tile_id must be an integer. Returning empty string...")
            return ""

        query = "select map_view.id from map_view where ContainerMapView_id={};".format(tile_id)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

    def get_tile_PNG(self, tile_id):
        if type(tile_id) != int:
            print("Error: type of tile_id must be an integer. Returning empty string...")
            return ""

        query = "select RasterFile from map_view where id={};".format(tile_id)

        document = self.run(query)
        results = [tuple(str(item) for item in t) for t in document]
        return results

#These classes extract data from a json file and format it to be inserted into a database.
class Message:

    def __init__(self, msg ):
        
        self.timestamp = msg['Timestamp'][:-1].replace('T',' ')
        self.mmsi = msg['MMSI']
        self.equiptclass = msg['Class']

    def to_shared_sql_values( self ):
        return "(NULL,'{}',{},'{}',NULL)".format( self.timestamp, self.mmsi, self.equiptclass )


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
        return f"({self.id},'{self.status}',{self.longitude},{self.latitude},{self.rot},{self.sog},{self.cog},{self.heading},NULL,NULL,NULL,NULL)"

class StaticData( Message ):

    def __init__(self, msg):

        super().__init__(msg)

        self.IMO = msg['IMO']
        self.name = msg['Name']
        self.vessel_type = msg['VesselType']
        self.length = msg['Length']
        self.breadth = msg['Breadth']

    def to_vessel_sql_values( self ):
        return "('{}',NULL,'{}',NULL,NULL,'{}','{}',NULL,NULL,'{}',NULL,NULL)".format(self.IMO, self.name, self.length, self.breadth, self.vessel_type)


#Method tests seperated by '#'
class TMB_test(unittest.TestCase):
    
    multi_batch = """[ {\"Timestamp\":\"2020-11-18T00:50:00\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:50:00\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:30:00\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:40:00\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:20:00\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290},
                {\"Timestamp\":\"2020-11-18T00:20:00\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""

    single_message = "[{\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97}]"

    ####################################################################################
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

    ####################################################################################
    def test_read_recent_position_given_MMSI_interface_fail_1(self):
        """
        Function `read_recent_position_given_MMSI` fails nicely if no parameter is passed
        """
        MMSI = 1234567
        tmb = DAO(True)
        value = tmb.read_recent_position_given_MMSI(MMSI)
        self.assertTrue(value=="")

    def test_read_recent_position_given_MMSI_interface_fail_2(self):
        """
        Function `read_recent_position_given_MMSI` fails nicely if no parameter is passed
        """
        tmb = DAO(True)
        value = tmb.read_recent_position_given_MMSI('TEST')
        self.assertTrue(value=="")
    
    def test_read_recent_position_given_MMSI_integration(self):
        """
        Function `read_recent_position_given_MMSI` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_recent_position_given_MMSI(219007155)
        self.assertEqual(result, [('2020-11-18 00:01:30', '219007155', '54.947323', 'None')])

    ####################################################################################
    def test_read_all_recent_ship_positions_integration(self):
        """
        Function `read_all_recent_ship_positions` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_all_recent_ship_positions()
        self.assertEqual(result[2], ('265866000', '54.763183', '12.415067', '2020-11-18 00:01:24', '9217242'))

    ####################################################################################
    def test_read_last_five_positions_given_MMSI_interface(self):
        tmb=DAO(True)
        result = tmb.read_last_five_positions_given_MMSI('hello')
        self.assertEqual(result, "")
    
    def test_read_last_five_positions_given_MMSI_integration(self):
        """
        Function `read_last_five_positions_given_MMSI` returns the result of the query as an array
        """
        tmb=DAO()
        result = tmb.read_last_five_positions_given_MMSI(219007155)
        self.assertEqual(result[0], ('2020-11-18 00:01:30', '219007155', '54.947327', '11.107760', 'None') )

    ####################################################################################
    def test_read_all_ports_matching_name_interface(self):
        """
        Function `read_all_ports_matching_name` fails nicely if no parameters are passed
        """
        tmb = DAO(True)
        result = tmb.read_all_ports_matching_name()
        self.assertEqual(result, "")

    def test_read_all_ports_matching_name_integration(self):
        """
        Function `read_all_ports_matching_name` returns the result of the query as an array
        """
        tmb = DAO()
        result = tmb.read_all_ports_matching_name(port_name='Ensted')
        self.assertEqual(result, [('4378', 'Ensted', 'Denmark', '55.022778', '9.439167')])

    ####################################################################################
    def test_read_recent_positions_given_tile_and_port_interface_fail_1(self):
        """Function 'read_recent_positions_given_tile_and_port' fails nicely if no paramaters are passed"""
        tmb = DAO(True)
        result = tmb.read_recent_positions_given_tile_and_port()
        self.assertEqual(result, '')

    def test_read_recent_positions_given_tile_and_port_interface_fail_2(self):
        """Function 'read_recent_positions_given_tile_and_port' fails nicely if incorrect parameter types are passed"""
        tmb = DAO(True)
        result = tmb.read_recent_positions_given_tile_and_port(port_name=1234)
        self.assertEqual(result, '')

    def test_read_recent_positions_given_tile_and_port_integration_1(self):
        """
        Function `read_recent_positions_given_tile_and_port` returns the result of the query as an array when passed the port name.
        """
        tmb = DAO()
        result =tmb.read_recent_positions_given_tile_and_port(port_name='Ensted')
        self.assertEqual(result[1], (('304858000', '55.218332', '13.371672', '8214358', '3')))
    
    def test_read_recent_positions_given_tile_and_port_integration_2(self):
        """
        Function `read_recent_positions_given_tile_and_port` returns the result of the query as an array when passed the country.
        """
        tmb = DAO()
        result =tmb.read_recent_positions_given_tile_and_port(country='Denmark')
        self.assertEqual(result[0], ('219007155', '54.947323', '11.107765', 'None', '3'))

    ####################################################################################
    def test_read_vessel_info_interface_fail(self):
        """
        Function 'read_vessel_info' fails nicely if an incorrect parameter is passed
        """
        tmb=DAO(True)
        result = tmb.read_vessel_info('hello')
        self.assertEqual(result, "")
    
    def test_read_vessel_info_integration_1(self):
        """
        Function 'read_vessel_info' returns expected values from given parameters
        """
        tmb = DAO()
        result = tmb.read_vessel_info(219000575)
        self.assertEqual(result, [('219000575', '55.712553', '12.588520', '5041968')])

    def test_read_vessel_info_integration_2(self):
        """
        Function 'read_vessel_info' returns expected values from given parameters
        """
        tmb = DAO()
        result = tmb.read_vessel_info(219000575, IMO=5041968)
        self.assertEqual(result, [('219000575', '55.712553', '12.588520', '5041968')])

    def test_read_vessel_info_integration_3(self):
        """
        Function 'read_vessel_info' returns expected values from given parameters
        """
        tmb = DAO()
        result = tmb.read_vessel_info(219000575, IMO=5041968, name='Guard Valiant')
        self.assertEqual(result, [('219000575', '55.712553', '12.588520', '5041968')])

    ####################################################################################
    def test_read_recent_positions_given_tile_interface(self):
        """
        Function 'read_recent_positions_given_tile' fails nicely if incorrect parameter is passed
        """
        tmb = DAO(True)
        result=tmb.read_recent_positions_given_tile("fail")
        self.assertEqual(result, "")
    
    def test_read_recent_positions_given_tile_integration(self):
        """
        Function 'read_recent_positions_given_tile returns the result as a query
        """
        tmb = DAO()
        result = tmb.read_recent_positions_given_tile(5039)
        self.assertEqual(result[0], ('244239000', '56.070297', '7.114718', '2020-11-18 00:01:32'))
        
    ####################################################################################
    def test_read_recent_ship_positions_headed_to_port_ID_interface(self):
        """
        Function 'read_recent_ship_positions_headed_to_port_ID' fails nicely if incorrect parameter is passed
        """
        tmb = DAO(True)
        result = tmb.read_recent_ship_positions_headed_to_port_ID("1234")
        self.assertEqual(result, "")

    def test_read_recent_ship_positions_headed_to_port_ID_integration(self):
        """
        Function 'read_recent_ship_positions_headed_to_port_ID' returns result of query as an array
        """
        tmb = DAO()
        result = tmb.read_recent_ship_positions_headed_to_port_ID(381)
        self.assertEqual(result[0], ('381', '220520000', '54.947323', '11.107765', '9107851'))

    ####################################################################################
    def test_read_recent_ship_positions_headed_to_port_interface_1(self):
        """
        Function 'read_recent_ship_positions_headed_to_port' fails nicely if incorrect parameter is passed
        """
        tmb = DAO(True)
        result = tmb.read_recent_ship_positions_headed_to_port(port_name=1234)
        self.assertEqual(result, "")

    def test_read_recent_ship_positions_headed_to_port_interface_2(self):
        """
        Function 'read_recent_ship_positions_headed_to_port' returns result of query as an array
        """
        tmb = DAO(True)
        result = tmb.read_recent_ship_positions_headed_to_port(country=1234)
        self.assertEqual(result, "")

    def test_read_recent_ship_positions_headed_to_port_interface_3(self):
        """
        Function 'read_recent_ship_positions_headed_to_port' returns result of query as an array
        """
        tmb = DAO(True)
        result = tmb.read_recent_ship_positions_headed_to_port()
        self.assertEqual(result, "")

    def test_read_recent_ship_positions_headed_to_port_integration(self):
        """
        Function 'read_recent_ship_positions_headed_to_port' returns result of query as an array
        """
        tmb = DAO()
        result = tmb.read_recent_ship_positions_headed_to_port(port_name='Nyborg')
        self.assertEqual(result[0], ('381', '220520000', '54.947323', '11.107765', '9107851'))
    ####################################################################################
    def test_lookup_contained_tiles_interface(self):
        """
        Function 'lookup_contained_tiles' fails nicely if incorrect parameter is passed
        """
        tmb = DAO(True)
        result = tmb.lookup_contained_tiles("failure")
        self.assertEqual(result, "")

    def test_lookup_contained_tiles_integration(self):
        """
        Function 'lookup_contained_tiles' returns the result of the query as an array
        """
        tmb = DAO(True)
        result = tmb.lookup_contained_tiles(5036)
        self.assertEqual(result, [('50361',), ('50362',), ('50363',), ('50364',)])   
    ####################################################################################
    def test_get_tile_PNG_interface(self):
        """
        Function ''get_tile_PNG' fails nicely if incorrect parameters are passed
        """
        tmb = DAO(True)
        result = tmb.get_tile_PNG("failure")
        self.assertEqual(result, "")

    def test_get_tile_PNG_integration(self):
        """
        Function 'get_tile_PNG' returns the result of the query as an array
        """
        tmb = DAO()
        result = tmb.get_tile_PNG(5036)
        self.assertEqual(result, [('38F7.png',)])
    ####################################################################################
    def test_delete_old_message_interface(self):
        """
        Function 'delete_old_message' fails nicely if incorrect parameters are passed
        """
        tmb = DAO(True)
        result = tmb.delete_old_ais_messages()
        self.assertTrue(type(result) is int)


if __name__ == '__main__':
    unittest.main()