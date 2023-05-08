import paho.mqtt.client as mqtt
from datetime import datetime
import tzlocal
import mysql.connector
import ast
import time
import logging 
import decimal
import Class_Definition
#creating and configure logger
logging.basicConfig(filename="logs.log",format='%(asctime)s %(message)s',filemode ='w')

#creating an object
logger = logging.getLogger()

#setting the level
logger.setLevel(logging.DEBUG)

try:
    conn = None
    conn = mysql.connector.connect(host='localhost',database='EMS',user='root',password='22@teneT')
    cursor = conn.cursor()
except Exception as error:
     logger.error(error)

# Initialize the start and end time variables
charging_start_time = None
charging_end_time = None

discharging_start_time = None
discharging_end_time = None

# Flag to check if start time and end time has been assigned
is_charg_start_time_assigned = False
is_discharg_start_time_assigned = False

is_charg_end_time_assigned = False
is_discharg_end_time_assigned = False
    

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
     
  client.subscribe(Class_Definition.Config.config['Topics']['Load'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Battery'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Charger'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['CV'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['CT'],1)
  client.subscribe(Class_Definition.Config.config['Topics']['Specs'],1)


def on_message(client, userdata, message):
    data = message.payload
    dict_str = data.decode("UTF-8")  #converting byte to dict
    data1 = ast.literal_eval(dict_str) #converting byte to dict
    print(data1)

    
    def charging(status,dischg_start_time):
         
         global is_charg_start_time_assigned,is_discharg_start_time_assigned,is_charg_end_time_assigned,is_discharg_end_time_assigned
         global charging_start_time ,charging_end_time ,discharging_start_time ,discharging_end_time
         
         chg_endtime = []
         is_discharg_end_time_assigned = False         
         if(data1["BPS"] == 1) :
             
             chg_endtime.append(data1['BRTC'])
             if not is_charg_start_time_assigned :
                 charging_start_time = data1['BRTC']
                 
                 discharging(data1["BPS"],charging_start_time)  #charging start time is discharging end time
                 
                 is_charg_start_time_assigned = True
                 
                 start_timestamp_unix = charging_start_time   #unix to local timestamp
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 start_timestamp_local = datetime.fromtimestamp(start_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
                 
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upschargingstarttime) VALUE (%s)"""
                 record_to_insert = (start_timestamp_local)
                 cursor.execute(insert_query,(record_to_insert,))
        
                 conn.commit()
                 
             
         elif (data1["BPS"] == 2 or data1["BPS"] == 0) :
             if not is_charg_end_time_assigned :
                 #charging_end_time = chg_endtime.pop()
                 charging_end_time = dischg_start_time
                 is_charg_end_time_assigned = True
                 
                 is_charg_start_time_assigned = False
                 
                 chg_endtime = []
                 
         
                 end_timestamp_unix = charging_end_time
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 end_timestamp_local = datetime.fromtimestamp(end_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime  
         
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upschargingendtime) VALUE (%s)"""
                 record_to_insert = (end_timestamp_local)
                 cursor.execute(insert_query, (record_to_insert,))
        
                 conn.commit()
         
                      
                 
    def discharging(status,charg_start_time) :
    
         global is_charg_start_time_assigned,is_discharg_start_time_assigned,is_charg_end_time_assigned,is_discharg_end_time_assigned
         global charging_end_time ,discharging_start_time ,discharging_end_time
         #global charging_start_time
             
         dchg_endtime = [] 
         is_charg_end_time_assigned = False
         if(data1["BPS"] == 2) :
             
             dchg_endtime.append(data1['BRTC'])
             if not is_discharg_start_time_assigned :
                 discharging_start_time = data1['BRTC']
                 
                 charging(data1["BPS"],discharging_start_time)   #discharging start time is charging end time
                 
                 is_discharg_start_time_assigned = True
                 
                 start_timestamp_unix = discharging_start_time    #unix to localtime
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 start_timestamp_local = datetime.fromtimestamp(start_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
                 #print(start_timestamp_local,121)
                 
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upsdischargingstartingtime) VALUES (%s)"""
                 record_to_insert = (start_timestamp_local)
                 cursor.execute(insert_query,(record_to_insert,))
        
                 conn.commit()
                 
             
         elif (data1["BPS"] == 1 or data1["BPS"] == 0) :
             if not is_discharg_end_time_assigned :
                 #discharging_end_time = dchg_endtime.pop()
                 discharging_end_time= charg_start_time
                 is_discharg_end_time_assigned = True
                 
                 is_discharg_start_time_assigned = False
                 
                 dchg_endtime = []
                 
                 end_timestamp_unix = discharging_end_time
                 local_timezone = tzlocal.get_localzone() # get pytz timezone
                 end_timestamp_local = datetime.fromtimestamp(end_timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime  
                 #print(end_timestamp_local,143)
   
         
                 insert_query = """ INSERT INTO EMSUPSChgDischgTime (upsdischargingendtime) VALUES (%s)"""
                 record_to_insert = (end_timestamp_local)
                 cursor.execute(insert_query, (record_to_insert,))
        
                 conn.commit()
    
    if message.topic == Class_Definition.Config.config['Topics']['Load']:
        print(data1)
        totalpower = data1["WTT"]
        powerrphase = data1["WTR"]
        poweryphase = data1["WTY"]
        powerbphase = data1["WTB"]
        powerfactoravarage= data1["PFA"]
        powerfactorrphase =data1["PFR"]
        powerfactoryphase=data1["PFY"]
        powerfactorbphase = data1["PFB"]
        totalapperantpower = data1["VAT"]
        apparantpowerrphase = data1["VAR"]
        apparantpoweryphase =data1["VAY"]
        apparantpowerbphase = data1["VAB"]
        voltagelinetolineavarage = data1["VLL"]
        voltageryphase = data1["VRY"]
        voltageybphase = data1["VYB"]
        voltagebrphase =data1["VBR"]
        voltagelineneutralavaerage = data1["VLN"]
        voltagerphase= data1["VR"]
        voltageyphase=data1["VY"]
        voltagebphase = data1["VB"]
        currenttotal = data1["CT"]
        currentrphase =data1["CR"]
        currentyphase = data1["CY"]
        currentbphase = data1["CB"]
        frequency = data1["FZ"]
        activeenergyeb =data1["WhEB"]
        apparantenergyeb = data1["VhEB"]
        activeenergydg = data1["WHDG"]
        apparantenergydg = data1["VhDG"]
        loadhourseb = data1["LHEB"]
        loadhoursdb = data1["LHDG"]
        timestamp_unix = data1['LRTC']


            #converting from unix timestamp to local time
        local_timezone = tzlocal.get_localzone() # get pytz timezone
        timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
        #print(timestamp_local,178) 
         
        #inserting into database
        insert_query = """ INSERT INTO EMSUPSLoad (loadtimestamp,totalpower,power_R_phase,power_Y_phase,power_B_phase,powerfactoravarage,powerfactor_R_phase,powerfactor_Y_phase,powerfactor_B_phase,totalapparantpower,apparantpower_R_phase,apparantpower_Y_phase,apparantpower_B_phase,voltagelinetolineaverage,voltage_RY_phase,voltage_YB_phase,voltage_BR_phase,voltagelinenuetralaverage,voltage_R_phase,voltage_Y_phase,voltage_B_phase,currenttotal,current_R_phase,current_Y_phase,current_B_phase,frequency,activeenergy_EB,apparantenergy_EB,activeenergy_DG,apparantenergy_DG,loadhours_EB,loadhours_DG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (timestamp_local,totalpower,powerrphase,poweryphase,powerbphase,powerfactoravarage,powerfactorrphase,powerfactoryphase,powerfactorbphase,totalapperantpower,apparantpowerrphase,apparantpoweryphase,apparantpowerbphase,voltagelinetolineavarage,voltageryphase,voltageybphase,voltagebrphase,voltagelineneutralavaerage,voltagerphase,voltageyphase,voltagebphase,currenttotal,currentrphase,currentyphase,currentbphase,frequency,activeenergyeb,apparantenergyeb,activeenergydg,apparantenergydg,loadhourseb,loadhoursdb)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()




    
    if message.topic == Class_Definition.Config.config['Topics']['Battery'] :
         print(data1)
         
             
           
         if(data1["BPS"] == 1) :
             battery_status =  "CHG"
             charging(data1["BPS"],data1['BRTC'])  #if status is 1  then goto charging funtion
             
         elif (data1["BPS"] == 2) :
             battery_status =  "DCHG"  
             discharging(data1["BPS"],data1['BRTC'])  #if status is 2 then goto discharging funtion
             
         elif(data1["BPS"] == 0) :
             battery_status =  "IDLE"

         else:
            battery_status = "Battery status is invalid. Please contact the battery team."
         #battery_status = data1["BPS"]    
         charge_energy = data1.get("BCHE", "")  # Get the value of "BCHE" key from data1 dictionary
         charge_energy = decimal.Decimal(charge_energy) if charge_energy else decimal.Decimal(0)
         discharge_energy = data1.get("BDCHE", "")  # Get the value of "BDCHE" key from data1 dictionary
         discharge_energy = decimal.Decimal(discharge_energy) if discharge_energy else decimal.Decimal(0)  # Convert to decimal format and handle empty string
         print(discharge_energy)  # Output the value of discharge_energy in decimal format

         pack_usable_soc = data1["BUSOC"]
         timestamp_unix = data1['BRTC']
         batteryvoltage = data1["BV"]
         batterycurrent = data1["BC"]
         contactorstatues = data1["BCS"]
         precontacrorstaues = data1['BPCS']
         packsoc = data1["BSOC"]
         negative_energy = data1.get("BDCHE", "")  # Get the value of "BDCHE" key from data1 dictionary
         if negative_energy:
             negative_energy = decimal.Decimal(negative_energy)  # Convert to decimal format
             if negative_energy > 0:  # Check if the value is positive
                 negative_energy = negative_energy * decimal.Decimal(-1)
                 print(negative_energy)  # Make the value negative
         else:
             negative_energy = decimal.Decimal(0)  # Handle empty string
             print(negative_energy)

         timestamp_unix = data1['BRTC']
         #converting from unix timestamp to local time
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
         #print(timestamp_local,178)
         current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')



         #inserting into database
         insert_query = """ INSERT INTO EMSUPSbattery (upstimestamp,upschargingenergy,upsdischargingenergy,pack_usable_soc,upsbatterystatus,batteryvoltage,batterycurrent,contactorstatues,precontactorstatues,packsoc,received_time,negative_energy) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,charge_energy,discharge_energy,pack_usable_soc,battery_status,batteryvoltage,batterycurrent,contactorstatues,precontacrorstaues,packsoc,current_time,negative_energy)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()


    if message.topic == Class_Definition.Config.config['Topics']['Charger'] :
         print(data1)
         chargevoltage1 = data1["CV1"]
         chargevoltage2 = data1["CV2"]
         chargecurrent1 = data1["CC1"]
         chargecurrent2 = data1["CC2"]

         timestamp_unix = data1['CRTC']
         timestamp_unix = int(timestamp_unix)
          # convert timestamp_unix from string to integer
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime

    
         
         #inserting into database
         insert_query = """ INSERT INTO EMSUPSCharger(chargetimestamp,chargevoltage1,chargevoltage2,chargecurrent1,chargecurrent2) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,chargevoltage1,chargevoltage2,chargecurrent1,chargecurrent2)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
    if message.topic == Class_Definition.Config.config['Topics']['CV'] :
        print(data1)
        cellsequncenumber = data1["CSEQ"]
        cellsfirstindex = data1["CFI"]
        cellslastindex = data1["CLI"]
        individualcellvolts = data1["CVOL"]
        timestamp_unix = data1['CVRTC']
        #converting from unix timestamp to local time
        local_timezone = tzlocal.get_localzone() # get pytz timezone
        timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
        #print(timestamp_local,178)
        insert_query = """ INSERT INTO EMSUPSCellvoltage(celltimestamp,cellsequencenumber,cellfirstindex,celllastindex,individualcellvolts) VALUES (%s,%s,%s,%s,%s)"""
        record_to_insert = (timestamp_local,cellsequncenumber,cellsfirstindex,cellslastindex,individualcellvolts)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()

         
    if message.topic == Class_Definition.Config.config['Topics']['CT'] :
         print(data1)
         temperaturesequencenumber = data1["TSEQ"]
         temperaturefirstindex = data1["TFI"]
         temperaturelastindex = data1["TLI"]
         individualcelltemperature = data1["CTEMP"]
         timestamp_unix = data1['CTRTC']
         #converting from unix timestamp to local time
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
         #print(timestamp_local,178)

         insert_query = """ INSERT INTO EMSUPSCelltemperature(celltemperaturetimestamp,temperaturesequencenumber,temperaturefirstindex,temperaturelastindex,individualtemperaturevolts) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,temperaturesequencenumber,temperaturefirstindex,temperaturelastindex,individualcelltemperature)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()

         
    if message.topic == Class_Definition.Config.config['Topics']['Specs'] :
         print(data1)
         packconfiguration = data1["CONFG"]
         celltotalsequencenumberspecs = data1["CTSEQ"]
         temperaturetotalsequencenumber = data1["TTSEQ"]
         softwareversion = data1["SVER"]
         hardwareversion = data1["HVER"]
         bmsversion = data1["BVER"]
         insert_query = """ INSERT INTO EMSUPSspecs(packconfigurationin_mXcXt,celltotalsequencenumber,temperaturetotalsequencenumber,softwareversion,hardwareversion,bmsversion) VALUES (%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (packconfiguration,celltotalsequencenumberspecs,temperaturetotalsequencenumber,softwareversion,hardwareversion,bmsversion)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()

while True:
    try:
        client = mqtt.Client()
        #client.username_pw_set(username="admin",password="admin@123")
        #client.connect("10.9.211.140",1883,60)
        client.username_pw_set(username=Class_Definition.Config.config['Broker_connection']['userName'],password=Class_Definition.Config.config['Broker_connection']['password'])
        client.connect(Class_Definition.Config.config['Broker_connection']['broker'],Class_Definition.Config.config['Broker_connection']['port'], Class_Definition.Config.config['Broker_connection']['keep_alive'])
        client.on_connect = on_connect
        client.on_message = on_message
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        break
    except:
        print("connection lost")
        time.sleep(5)
        client.reconnect()