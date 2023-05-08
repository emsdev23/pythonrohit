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

        current_time = datetime.now()
        current_minute = current_time.minute
        current_second = current_time.second

        now = datetime.now()


        if current_minute in [0,30,45,0]:
            # Execute the query
            query = "SELECT MVPPolling.mvpnum, SUM(energy_difference) AS total_energy_difference, (SELECT MAX(cumulative_energy) FROM (SELECT mvpnum, SUM(energy_difference) AS total_energy_difference, SUM(SUM(energy_difference)) OVER (ORDER BY MVPPolling.mvpnum) AS cumulative_energy FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 15 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) MVPPolling WHERE energy_difference IS NOT NULL GROUP BY MVPPolling.mvpnum) t) AS cumulative_energy FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 15 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) MVPPolling WHERE energy_difference IS NOT NULL GROUP BY MVPPolling.mvpnum;"
            source_cur.execute(query)

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                mvpnum,total_energy_difference,cumulative_energy= row
                insert_rows.append((mvpnum,total_energy_difference,cumulative_energy))
                print(row)

            # Insert the rows into the destination database
            insert_query = "INSERT INTO acmeterreadingquarterly (mvpnum,energy_difference,cumulative_energy) VALUES (%s, %s, %s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
        if current_time.minute == 0:
              # Execute the query
            query = "SELECT MVPPolling.mvpnum, SUM(energy_difference) AS total_energy_difference, (SELECT MAX(cumulative_energy) FROM (SELECT mvpnum, SUM(energy_difference) AS total_energy_difference, SUM(SUM(energy_difference)) OVER (ORDER BY MVPPolling.mvpnum) AS cumulative_energy FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 60 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) MVPPolling WHERE energy_difference IS NOT NULL GROUP BY MVPPolling.mvpnum) t) AS cumulative_energy FROM (SELECT mvpnum, acmeterenergy - LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime) AS energy_difference FROM MVPPolling WHERE polledTime BETWEEN DATE_SUB(NOW(), INTERVAL 60 MINUTE) AND NOW() AND mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) MVPPolling WHERE energy_difference IS NOT NULL GROUP BY MVPPolling.mvpnum;;"
            source_cur.execute(query)

            # Fetch all the rows from the source database
            rows = source_cur.fetchall()
            

            # Create a list to store the rows to be inserted into the destination database
            insert_rows = []

            # Loop through the rows and append the values to the insert_rows list
            for row in rows:
                mvpnum,total_energy_difference,cumulative_energy= row
                insert_rows.append((mvpnum,total_energy_difference,cumulative_energy))
                print(row)

            # Insert the rows into the destination database
            insert_query = "INSERT INTO acmeterreadinghourly (mvpnum,energy_difference,cumulative_energy) VALUES (%s, %s, %s);"
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