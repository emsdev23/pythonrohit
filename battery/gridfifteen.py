import time
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import pooling

# Connect to the MySQL database
while True:
    try:
        source_pool = mysql.connector.pooling.MySQLConnectionPool(
            host="121.242.232.151",
            user="bmsrouser6",
            password="bmsrouser6@151",
            database="bmsmgmt_olap_prod_v13"
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
        
        # Execute the query
        query = ";"
        source_cur.execute(query)

        # Fetch all the rows from the source database
        rows = source_cur.fetchall()

        # Create a list to store the rows to be inserted into the destination database
        insert_rows = []

        # Loop through the rows and append the values to the insert_rows list
        for row in rows:
            device_id, polled_timestamp, energy_value, energy_diff, cumulative_energy,totalcummalativediff= row
            insert_rows.append((device_id, polled_timestamp, energy_value, energy_diff, cumulative_energy,totalcummalativediff))
            print(row)

        # Insert the rows into the destination database
        insert_query = "SELECT MVPPolling.mvpnum, SUM(MVPPolling.acmeterenergy - prev.acmeterenergy) AS RowEnergyDifference, (SELECT SUM(acmeterenergy) FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 60 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) AS TotalEnergyDifference FROM MVPPolling INNER JOIN (SELECT mvpnum, MAX(polledTime) AS max_pollingtime, acmeterenergy FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 60 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') GROUP BY mvpnum, acmeterenergy) prev ON MVPPolling.mvpnum = prev.mvpnum AND MVPPolling.polledTime = prev.max_pollingtime WHERE MVPPolling.mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') GROUP BY MVPPolling.mvpnum;"
        dest_cur.executemany(insert_query, insert_rows)
        print(insert_query)
        dest_db.commit()

        source_cur.close()
        source_db.close()
        dest_cur.close()
        dest_db.close()

        # Sleep for 20 minutes
        time.sleep(1100)
        
    except Exception as e:
        print("An error occurred: ", e)
        time.sleep(12)