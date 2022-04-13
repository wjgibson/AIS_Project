import unittest
import mysql.connector
from mysql.connector import errorcode

import sys
import operator
from datetime import *

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