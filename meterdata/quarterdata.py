import mysql.connector
import time
import datetime



while True:
    # Connect to the source database
    source_db = mysql.connector.connect(
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

    # Get the current time
    current_time = datetime.datetime.now()
    current_minute = current_time.minute
    current_second = current_time.second

    source_cur = source_db.cursor()
    source_cur.execute("SET GLOBAL max_connections = 1000;")

    # Get the destination cursor
    dest_cur = dest_db.cursor()


    # Acquire the lock

    try:
         if current_minute in [15,33,34,35,36,45,0]:
            source_cur.execute("""SELECT 
    inverterdeviceid,
    inverterenergy,
    inverteractivepower,
    invertertimestamp,
    inverterenergy - 
        (SELECT inverterenergy 
         FROM EMSInverterData sub 
         WHERE sub.inverterdeviceid = EMSInverterData.inverterdeviceid 
           AND sub.invertertimestamp < EMSInverterData.invertertimestamp 
           AND sub.invertertimestamp >= DATE_SUB(EMSInverterData.invertertimestamp, INTERVAL 15 MINUTE)
         ORDER BY invertertimestamp DESC 
         LIMIT 1) AS energy_difference,
    SUM(inverteractivepower) AS cumulativeactivepower
FROM 
    EMSInverterData
WHERE 
    inverterdeviceid IN ('1', '2', '3', '4', '5', '6', '7', '8')
    AND invertertimestamp >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
GROUP BY 
    inverterdeviceid
ORDER BY 
   inverterdeviceid, invertertimestamp DESC  
LIMIT 8;""")


            rows = source_cur.fetchall()

            # Print the result for debugging
            

            insert_rows = []

            for row in rows:
                inverterdeviceid,inverterenergy,inverteractivepower,invertertimestamp,energy_difference,cumulativeactivepower = row
                insert_rows.append((inverterdeviceid,inverterenergy,inverteractivepower,invertertimestamp,energy_difference,cumulativeactivepower))
                print(row)


            insert_query = "INSERT INTO inverterprocessing(inverterdeviceid,inverterenergy,inverteractivepower,invertertimestamp,energy_difference,cumulativeactivepower) VALUES (%s, %s, %s, %s, %s,%s);"
            dest_cur.executemany(insert_query, insert_rows)
            dest_db.commit()
            time.sleep(50)
        
    except Exception as e:
        print(f"Error: {e}")
        dest_db.rollback()
        
    finally:
        source_cur.close()
        dest_cur.close()
        source_db.close()
        dest_db.close()