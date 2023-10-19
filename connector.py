import mysql.connector
import configparser
import sys


config = configparser.ConfigParser()
try:
    config.read('config.ini')
except Exception as e:
    print('could not read configuration file:' + str(e))
    sys.exit()
class MyDatabase:
    def __init__(self):
        self.db = None

    def connect_to_db(self):
        try:
            # Connect to the MySQL server without specifying a database
            self.db = mysql.connector.connect(
                host=config['CONFIG']['host'],
                user=config['CONFIG']['user'],
                password=config['CONFIG']['password']
            )

            dbcursor = self.db.cursor()
            dbcursor.execute("SHOW DATABASES")
            databases = [x[0] for x in dbcursor]

            if "etlpy" not in databases:
                # Create the "ETLpy" database if it doesn't exist
                dbcursor.execute("CREATE DATABASE etlpy")
                print("Created 'etlpy' database.")

            # Connect to the "ETLpy" database
            self.db = mysql.connector.connect(
                host=config['CONFIG']['host'],
                user=config['CONFIG']['user'],
                password=config['CONFIG']['password'],
                database=config['CONFIG']['database']
            )
            return self.db

        except mysql.connector.Error as err:
            print("Error: {}".format(err))
            return None

mydb = MyDatabase()
conn = mydb.connect_to_db()

if conn:
    print("Connected to MySQL database")
else:
    print("Failed to connect to MySQL database")