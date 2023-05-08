import mysql.connector.pooling
import time
from datetime import datetime, timedelta

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
        query = "SELECT t.acmetersubsystemid, t.acmeterpolledtimestamp, CAST(t.acmeterenergy AS DECIMAL(18,2)), CAST(CASE WHEN t.acmeterenergy REGEXP '^[0-9]+\.?[0-9]*$' THEN t.acmeterenergy - t.latest_energy ELSE 0 END AS DECIMAL(18,2)), c.cumulative_diff FROM (SELECT acmetersubsystemid, acmeterpolledtimestamp, acmeterenergy, COALESCE(LAG(acmeterenergy) OVER (PARTITION BY acmetersubsystemid ORDER BY acmeterpolledtimestamp ASC), acmeterenergy) AS latest_energy FROM acmeterreadings WHERE acmetersubsystemid IN (1135, 1167, 350, 358) AND acmeterpolledtimestamp >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AND acmeterpolledtimestamp <= NOW()) t JOIN (SELECT acmetersubsystemid, acmeterpolledtimestamp, SUM(CASE WHEN acmeterenergy REGEXP '^[0-9]+\.?[0-9]*$' THEN acmeterenergy - latest_energy ELSE 0 END) OVER (PARTITION BY acmetersubsystemid ORDER BY acmeterpolledtimestamp ASC) AS cumulative_diff FROM (SELECT acmetersubsystemid, acmeterpolledtimestamp, acmeterenergy, COALESCE(LAG(acmeterenergy) OVER (PARTITION BY acmetersubsystemid ORDER BY acmeterpolledtimestamp ASC), acmeterenergy) AS latest_energy FROM acmeterreadings WHERE acmetersubsystemid IN (1135, 1167, 350, 358) AND acmeterpolledtimestamp >= DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d 00:00:00') AND acmeterpolledtimestamp <= NOW()) t2) c ON t.acmetersubsystemid = c.acmetersubsystemid AND t.acmeterpolledtimestamp = c.acmeterpolledtimestamp ORDER BY acmeterpolledtimestamp DESC LIMIT 10;"
        source_cur.execute(query)

        # Fetch all the rows from the source database
        rows = source_cur.fetchall()

        # Create a list to store the rows to be inserted into the destination database
        insert_rows = []

        # Loop through the rows and append the values to the insert_rows list
        for row in rows:
            device_id, polled_timestamp, energy_value, energy_diff, cumulative_energy = row
            insert_rows.append((device_id, polled_timestamp, energy_value, energy_diff, cumulative_energy))
            print(row)

        # Insert the rows into the destination database
        insert_query = "INSERT INTO acmeterreading(acmetersubsystemid, acmeterpolledtimestamp, acmeterenergy_cast, energy_diff, cumulative_energy) VALUES (%s, %s, %s, %s, %s);"
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

       

