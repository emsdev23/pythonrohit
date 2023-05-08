import mysql.connector
import time
import datetime

# Connect to the MySQL database
source_db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="22@teneT",
    database="EMS"
)

# Set the max_connections parameter to a higher value
source_cur = source_db.cursor()
source_cur.execute("SET GLOBAL max_connections = 1000;")
source_db.commit()

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

    # Get the destination cursor
    dest_cur = dest_db.cursor()

    # Acquire the lock

    try:
         if current_minute in [15,30,45,0]:
            source_cur.execute("""
                SELECT 
  (SELECT SUM(meterpower) 
   FROM EMSMeterData 
   WHERE metertimestamp >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)) AS meterpower_sum,
  sign((SELECT meterenergy 
        FROM EMSMeterData 
        ORDER BY metertimestamp DESC 
        LIMIT 1) -
       (SELECT meterenergy 
        FROM EMSMeterData 
        WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 15 MINUTE) 
        ORDER BY metertimestamp DESC 
        LIMIT 1)) * ABS((SELECT meterenergy 
                          FROM EMSMeterData 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1) -
                         (SELECT meterenergy 
                          FROM EMSMeterData 
                          WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 15 MINUTE) 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1)) AS meterenergy_diff,
  (SELECT SUM(wmsirradiation) 
   FROM EMSWMSData 
   WHERE wmstimestamp >= DATE_SUB(NOW(), INTERVAL 15 MINUTE)) AS wmsirradiation_sum

    """)

            result = source_cur.fetchone()

            # Print the result for debugging
            print(f"Result: {result}")

            if result[0] is not None:
                cumulativepower = result[0]
                instant = result[1]
                total_wmsirradiation = result[2]
                wmsirradiationkwh = total_wmsirradiation/60000
                print()

                # Insert the average energy and cumulative power into the destination table
                dest_cur.execute("""
                INSERT INTO QuarterlyMeterData (cumulativepower,instantaneousenergy,wmsirradiation)
                VALUES (%s,%s,%s)
                """, (cumulativepower,instant,wmsirradiationkwh))

                # Commit the changes to the destination database
                dest_db.commit()
            else:
                print("No data found in EMS.EMSMeterData table for the last 15 minutes.")
                # Sleep for a while before restarting the loop
                time.sleep(1)  # Sleep for 60 seconds before restarting the loop
                continue  # Exit the current iteration of the loop and break out of the loop

         
         

         if current_time.minute == 0:
            source_cur.execute("""SELECT 
  (SELECT SUM(meterpower) 
   FROM EMSMeterData 
   WHERE metertimestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)) AS meterpower_sum,
  sign((SELECT meterenergy 
        FROM EMSMeterData 
        ORDER BY metertimestamp DESC 
        LIMIT 1) -
       (SELECT meterenergy 
        FROM EMSMeterData 
        WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 1 HOUR) 
        ORDER BY metertimestamp DESC 
        LIMIT 1)) * ABS((SELECT meterenergy 
                          FROM EMSMeterData 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1) -
                         (SELECT meterenergy 
                          FROM EMSMeterData 
                          WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 1 HOUR) 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1)) AS meterenergy_diff,
  (SELECT SUM(wmsirradiation) 
   FROM EMSWMSData 
   WHERE wmstimestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)) AS wmsirradiation_sum;

""")

            # Fetch the result of the query
            result = source_cur.fetchone()

            # Print the result for debugging
            print(f"Result: {result}")

            if result[0] is not None:
                cumulativepower = result[0]
                instant = result[1]
                total_wmsirradiation = result[2]
                wmsirradiationkwh = total_wmsirradiation/60000
        

                # Insert the average energy and cumulative power into the destination table

                dest_cur.execute("""
            INSERT INTO HourlyMeterData(cummulativemeterpower,instantaneousenergy,wmsirradiation)
            VALUES (%s,%s,%s)
            """, (cumulativepower,instant,wmsirradiationkwh))

               # Commit the changes to the destination database
                dest_db.commit()

            else:
                print("No data found in EMS.EMSMeterData table for the last 15 minutes.")
                time.sleep(1)  # Sleep for 60 seconds before restarting the loop
                continue  # Exit the current iteration of the loop and break out of the loop

         if current_time.minute == 0:
            source_cur.execute("""SELECT 
  (SELECT SUM(meterpower) 
   FROM EMSMeterData 
   WHERE metertimestamp >= DATE_SUB(NOW(), INTERVAL 1 DAY)) AS meterpower_sum,
  sign((SELECT meterenergy 
        FROM EMSMeterData 
        ORDER BY metertimestamp DESC 
        LIMIT 1) -
       (SELECT meterenergy 
        FROM EMSMeterData 
        WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 1 DAY) 
        ORDER BY metertimestamp DESC 
        LIMIT 1)) * ABS((SELECT meterenergy 
                          FROM EMSMeterData 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1) -
                         (SELECT meterenergy 
                          FROM EMSMeterData 
                          WHERE metertimestamp <= DATE_SUB(NOW(), INTERVAL 1 DAY) 
                          ORDER BY metertimestamp DESC 
                          LIMIT 1)) AS meterenergy_diff,
  (SELECT SUM(wmsirradiation) 
   FROM EMSWMSData 
   WHERE wmstimestamp >= DATE_SUB(NOW(), INTERVAL 1 DAY)) AS wmsirradiation_sum


""")

            # Fetch the result of the query
            result = source_cur.fetchone()

            # Print the result for debugging
            print(f"Result: {result}")

            if result[0] is not None:
                cumulativepower = result[0]
                instant = result[1]
                total_wmsirradiation = result[2]
                wmsirradiationkwh = total_wmsirradiation/60000
        

                # Insert the average energy and cumulative power into the destination table

                dest_cur.execute("""
            INSERT INTO DailyMeterData (cummulativemeterpower,instantaneousenergy,wmsirradiation)
            VALUES (%s, %s ,%s)
            """, (cumulativepower,instant,wmsirradiationkwh))

               # Commit the changes to the destination database
                dest_db.commit()

            else:
                print("No data found in EMS.EMSMeterData table for the last 15 minutes.")
                time.sleep(1)  # Sleep for 60 seconds before restarting the loop
                continue  # Exit the current iteration of the loop and break out of the loop

        

         dest_db.commit()
         time.sleep(60)
        
    except Exception as e:
        print(f"Error: {e}")
        dest_db.rollback()
        
    finally:
        source_cur.close()
        dest_cur.close()
        source_db.close()
        dest_db.close()


