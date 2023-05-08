import time
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import pooling

# Connect to the MySQL database
while True:
    try:
        source_pool = mysql.connector.pooling.MySQLConnectionPool(
            host="localhost",
            user="root",
            password="22@teneT",
            database="EMS"
        )
        
        # Connect to the destination database
        dest_db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="22@teneT",
            database="meterdata"
        )

        source_db = source_pool.get_connection()
        
        # Create a cursor for the source database
        source_cur = source_db.cursor()

        # Create a cursor for the destination database
        dest_cur = dest_db.cursor()

        current_time = datetime.now()
        current_minute = current_time.minute
        current_second = current_time.second

        now = datetime.now()


        if current_minute in [13,15,30,45,0]:
            # Execute the query
            query = "SELECT thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve, thermalstorage_linepressure, SUM(coolingenergy_consumption) AS cooling_energy_consumption_last_15_minutes FROM EMSThermalstorage WHERE received_time >= NOW() - INTERVAL 15 MINUTE;"
            source_cur.execute(query)

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve ,thermalstorage_linepressure,cooling_energy_consumption_last_15_minutes = row
                insert_rows.append((thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve ,thermalstorage_linepressure,cooling_energy_consumption_last_15_minutes))
                print(row)

            # Insert the rows into the destination database
            insert_query = "INSERT INTO thermalstoragequarterly(thermalstoredwatertemperature,H_valve,adpvalve,bdpvalve,thermalstoragelinepressure,coolingenergyconsumption) VALUES (%s, %s, %s,%s,%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
        if current_time.minute == 13:
              # Execute the query
            query = "SELECT thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve, thermalstorage_linepressure, SUM(coolingenergy_consumption) AS cooling_energy_consumption_last_15_minutes FROM EMSThermalstorage WHERE received_time >= NOW() - INTERVAL 60 MINUTE;"
            source_cur.execute(query)

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve ,thermalstorage_linepressure,cooling_energy_consumption_last_15_minutes= row
                insert_rows.append((thermalstorage_storedwatertemperature, H_valve, adpvalve, bdpvalve ,thermalstorage_linepressure,cooling_energy_consumption_last_15_minutes))
                print(row)

            # Insert the rows into the destination database
            insert_query = "INSERT INTO thermalstoragehourly(thermalstoredwatertemperature,H_valve,adpvalve,bdpvalve,thermalstoragelinepressure,coolingenergyconsumption) VALUES (%s, %s, %s,%s,%s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
        
        source_cur.close()
        source_db.close()
        dest_cur.close()
        dest_db.close()

        # Sleep for 10 seconds
        time.sleep(46)
        
    except Exception as e:
        print("An error occurred: ", e)
        time.sleep(12)
