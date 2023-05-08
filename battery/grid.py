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
            database="bmsmgmtprodv13"
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
        query = "SELECT t.mvpnum, t.polledTime, CAST(t.acmeterenergy AS DECIMAL(18,2)), CAST(CASE WHEN t.acmeterenergy REGEXP '^[0-9]+\.?[0-9]*$' THEN t.acmeterenergy - t.latest_energy ELSE 0 END AS DECIMAL(18,2)), c.cumulative_diff, (SELECT SUM(cumulative_diff) FROM (SELECT mvpnum, MAX(polledTime) AS last_timestamp, SUM(CASE WHEN acmeterenergy REGEXP '^[0-9]+\.?[0-9]*$' THEN acmeterenergy - latest_energy ELSE 0 END) AS cumulative_diff FROM (SELECT mvpnum, polledTime, acmeterenergy, COALESCE(LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime ASC), acmeterenergy) AS latest_energy FROM MVPPolling WHERE mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') AND polledTime >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AND polledTime <= NOW()) t2 GROUP BY mvpnum) c2 WHERE mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4')) as total_cumulative_diff FROM (SELECT mvpnum, polledTime, acmeterenergy, COALESCE(LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime ASC), acmeterenergy) AS latest_energy FROM MVPPolling WHERE mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') AND polledTime >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AND polledTime <= NOW()) t JOIN (SELECT mvpnum, MAX(polledTime) AS last_timestamp, SUM(CASE WHEN acmeterenergy REGEXP '^[0-9]+\.?[0-9]*$' THEN acmeterenergy - latest_energy ELSE 0 END) AS cumulative_diff FROM (SELECT mvpnum, polledTime, acmeterenergy, COALESCE(LAG(acmeterenergy) OVER (PARTITION BY mvpnum ORDER BY polledTime ASC), acmeterenergy) AS latest_energy FROM MVPPolling WHERE mvpnum IN ('MVP1', 'MVP2', 'MVP3', 'MVP4') AND polledTime >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AND polledTime <= NOW()) t2 GROUP BY mvpnum) c ON t.mvpnum = c.mvpnum AND t.polledTime = c.last_timestamp;"
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
        insert_query = "INSERT INTO acmeterreading(acmetersubsystemid, acmeterpolledtimestamp, acmeterenergy_cast, energy_diff, cumulative_energy,totalcummalativeenergy) VALUES (%s, %s, %s, %s, %s,%s);"
        dest_cur.executemany(insert_query, insert_rows)
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