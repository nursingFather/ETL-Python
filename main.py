import os
import sys
import petl
from connector import config, conn
import mysql.connector
import requests
import datetime
import json
import decimal

startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['host']
destDatabase = config['CONFIG']['database']
PASSWORD = config['CONFIG']['password']
USER= config['CONFIG']['user']



# requesting data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('could not make request: ' + str(e))
    sys.exit()

#  initialize the list of lists for data storage
BOCDates = []
BOCRates = []
# process BOC json object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)

    # extract observation data into columns array
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row["FXUSDCAD"]["v"]))

        #create a petl table from mcolumn arrray and rename the columns.
        exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

        # load expense document
        try:
            expenses = petl.io.xlsx.fromxlsx("Expenses.xlsx", sheet='Github')
        except Exception as e:
            print('could not open expenses.xlsx: ' + str(e))
            sys.exit()

        expenses = petl.outerjoin(exchangeRates, expenses, key='date')

        #fill down missing value
        expenses = petl.filldown(expenses, 'rate')
        #remove date with no expenses
        expenses = petl.select(expenses, lambda rec: rec.USD != None)

        # add CDN Column
        expenses = petl.addfield(expenses, "CAD", lambda rec: decimal.Decimal(rec.USD) * rec.rate)

        try:
            db = mysql.connector.connect(
                host=destServer,
                database=destDatabase,
                password=PASSWORD,
                user=USER
        )
            cursor = db.cursor()
        except Exception as e:
            print("Failed to connect to the database")
            sys.exit()
        # populating Expense databasbe table
        try:
            # Your database operations here (inserts, updates, etc.)

            petl.io.todb(expenses, cursor, 'expenses')

            db.commit()  # Commit changes to the database
        except Exception as e:
            print('Could not write to the database: ' + str(e))
        finally:
            cursor.close()  # Close the cursor
            db.close()




