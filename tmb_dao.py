import unittest
import json
import mysqlutils as mu

class TMB_DAO:

    config = "connection_data.conf"

    def __init__(self, stub=False):
        self.is_stub=stub
        

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

        with mu.MySQLConnectionManager( self.config ) as con:
            
            cursor = con.cursor();

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





class TMBTest( unittest.TestCase ):

    config = "connection_data.conf"

    batch = """[ {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":304858000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.218332,13.371672]},\"Status\":\"Under way using engine\",\"SoG\":10.8,\"CoG\":94.3,\"Heading\":97},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"AtoN\",\"MMSI\":992111840,\"MsgType\":\"static_data\",\"IMO\":\"Unknown\",\"Name\":\"WIND FARM BALTIC1NW\",\"VesselType\":\"Undefined\",\"Length\":60,\"Breadth\":60,\"A\":30,\"B\":30,\"C\":30,\"D\":30},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":219005465,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.572602,11.929218]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0,\"CoG\":298.7,\"Heading\":203},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":257961000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[55.00316,12.809015]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":0.2,\"CoG\":225.6,\"Heading\":240},
                {\"Timestamp\":\"2020-11-18T00:00:00.000Z\",\"Class\":\"Class A\",\"MMSI\":376503000,\"MsgType\":\"position_report\",\"Position\":{\"type\":\"Point\",\"coordinates\":[54.519373,11.47914]},\"Status\":\"Under way using engine\",\"RoT\":0,\"SoG\":7.6,\"CoG\":294.4,\"Heading\":290} ]"""



    def test_insert_message_batch_interface_1( self  ):
        """
        Function `insert_message_batch` takes an array of messages as an input.
        """
        tmb = TMB_DAO(True) 
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 

    def test_insert_message_batch_interface_2( self  ):
        """
        Function `insert_message_batch` fails nicely if input is still a string
        """
        tmb = TMB_DAO(True) 
        inserted_count = tmb.insert_message_batch( self.batch )
        self.assertEqual( inserted_count, -1) 


    def test_insert_message_batch( self  ):
        """
        Function `insert_message_batch` inserts messages in the MySQL table
        """
        tmb = TMB_DAO()
        array = json.loads( self.batch )
        inserted_count = tmb.insert_message_batch( array )
        self.assertTrue( type(inserted_count) is  int and inserted_count >=0) 


    def test_position_report_creation( self ):

        pr = {"Timestamp":"2020-11-18T00:00:00.000Z","Class":"Class A","MMSI":304858000,"MsgType":"position_report","Position":{"type":"Point","coordinates":[55.218332,13.371672]},"Status":"Under way using engine","SoG":10.8,"CoG":94.3,"Heading":97}
        
        tmb = TMB_DAO(True)
        pr = PositionReport( pr )  
        self.assertEqual( pr.timestamp, "2020-11-18 00:00:00.000")
       



    def setUp(self):
        """ Create the schema """
        with mu.MySQLConnectionManager( self.config ) as con:
            cursor = con.cursor()

            for result in cursor.execute( """ 
            drop database if exists AISUnitTest; 
            create database AISUnitTest; use AISUnitTest ;
            
            create table AIS_MESSAGE(
            Id mediumint unsigned auto_increment,
                Timestamp datetime,
                MMSI int,
                Class enum('Class A','Class B','AtoN','Base Station', 'SAR Airborne', 'Search and Rescue Transponder', 'Man Overboard Device'),
                Vessel_IMO mediumint unsigned,
                #foreign key (Vessel_IMO) references VESSEL(IMO),
                primary key (Id));

            create table POSITION_REPORT(
                AISMessage_Id mediumint unsigned,
                NavigationalStatus varchar(40),
                Longitude decimal(9,6),
                Latitude decimal(8,6),
                RoT decimal(4,1),
                SoG decimal(4,1),
                CoG decimal(4,1),
                Heading smallint,
                LastStaticData_Id mediumint unsigned,
                MapView1_Id mediumint,
                MapView2_Id mediumint,
                MapView3_Id mediumint,
                foreign key (AISMessage_Id) references AIS_MESSAGE(Id),
                #foreign key (LastStaticData_Id) references STATIC_DATA(AISMessage_Id),
                #foreign key (MapView1_Id) references MAP_VIEW(Id),
                #foreign key (MapView2_Id) references MAP_VIEW(Id),
                #foreign key (MapView3_Id) references MAP_VIEW(Id),
                primary key (AISMessage_Id)
            );

            """, multi=True):
                print( cursor.statement )


if __name__ == '__main__':
    unittest.main()





