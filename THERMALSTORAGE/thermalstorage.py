import paho.mqtt.client as mqtt
import mysql.connector
import datetime
import json
import logging
import time

#creating and configure logger
logging.basicConfig(filename="logs.log",format='%(asctime)s %(message)s',filemode ='w')

#creating an object
logger = logging.getLogger()

#setting the level
logger.setLevel(logging.DEBUG)

try:
  mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password ="22@teneT",
    database = "EMS"
    )
  if(mydb):
    print('my sql connected')
  else:
    print('my sql not connected')

  cursorObject = mydb.cursor()

except Exception as error:
     logger.error(error)




def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("swadha/IITMRPTS00000002/IITMRPTS/log",1)

def on_message(client, userdata, message):
  data= message.payload
  data_str = message.payload.decode("utf-8")
  data_dict = json.loads(data_str)
  print(data)

  if message.topic == "swadha/IITMRPTS00000002/IITMRPTS/log":
    timestamp = data_dict["DAT"]
    date = datetime.datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    #current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    mode = data_dict["MOD"]
    vfd = data_dict["VFS"]
    minf = data_dict["MXF"]
    maxf = data_dict["MNF"]
    frequency = data_dict["FRQ"]
    setflow = data_dict["SFW"]
    actuatorposition = data_dict["WAP"]
    actuator = data_dict["ANF"]
    pressureconstant = data_dict["PCT"]
    wateractuatordirection = data_dict["WAD"]
    flowtype = data_dict["FLT"]
    flowsetting = data_dict["FTP"]
    tstobuilding = data_dict["DFLR"]
    buildingtots = data_dict["ACFLR"]
    kitchofftemps = data_dict["KOT"]
    coolingenergy = data_dict["CEC"]
    tsinlet = data_dict["TSIN"]
    tsoutlet = data_dict["TSOU"]
    tslinepressure = data_dict["CLP"]
    tsstoredwatertemperature = data_dict["TSWT"]
    vfdcurrent = data_dict["AMP"]
    vfdvoltage = data_dict["VOL"]
    cummulativeflow = data_dict["TFL"]
    flowvalueaverage = data_dict["FVA"]
    overallvaluesaverage = data_dict["OVA"]
    linepressurelowerlimit = data_dict["LPLL"]
    linepresureupperlimit = data_dict["LPUL"]
    setflowtolerance = data_dict["SFT"]
    linepressurelowerlimittolerance = data_dict["LPLLT"]
    linepressureupperlimittolerance = data_dict["LPULT"]
    frequencypressurepidconstant = data_dict["FPPC"]
    frequencyflowpidconstant = data_dict["FFPC"]
    valvepressurepidconstant = data_dict["VPPC"]
    valveflowpidconstant = data_dict["VFPC"]
    allowablefrequency = data_dict["AF"]
    valopen = data_dict["VALOPEN"]
    valclose = data_dict["VALCLOSE"]
    cutofftemp= data_dict["COT"]
    bdpvalve = data_dict["BDPV"]
    bdpvalvefeedback = data_dict["BDPVF"]
    adpvalve =data_dict["ADPV"]
    adpvalvefeedback= data_dict["ADPVF"]
    hvalve = data_dict["HV"]
    hvalvefeedback = data_dict["HVF"]
    vfdalarm =data_dict["VFDA"]
    kickofftempenable =data_dict["KOTED"]
    kickofftemp = data_dict["KOT"]
    cutofftemp_enable= data_dict["COTED"]
    maxvalueposition = data_dict["MVP"]
    errorcode = data_dict["ERROR"]

    try:
      mysql_insert_query = "INSERT INTO EMSTHERMALSTORAGE (thermaldatetime, mode, vfdstatues, maxfrequency, minfrequency, frequency, setflow, actutatorposition, actutator, pressureconstant, wateractutator_direction, flowmetertype,flowsetting, thermalstorage_to_buildingflowrate, building_to_thermalstorage_flowrate, coolingenergy_consumption, thermalstorage_inlet, thermalstorage_outlet, thermalstorage_linepressure, thermalstorage_storedwatertemperature, vfdcurrent, vfdvoltage, cummulativeflow, flowvalve_average, overallvalve_average, linepressure_lowerlimit, linepressure_upperlimit, setflow_tolerence, linepressure_lowerlimittolerence,linepresssure_upperlimittolerence, frequencypressure_pidconstant, frequencyflow_pidconstant, valvepressure_pidconstant, valveflow_pidconstant, allowable_frequency, cutofftemp, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, H_valve, H_valvefeedback, vfdalarm,kichofftemp_enableORdisable, cutofftemp_enableordisable,maximumvalveposition,Error_at,kitchoff_temp,valve_open,valve_close) VALUES (%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
      record_to_insert = (date, mode, vfd, maxf / 100, minf / 100, frequency / 100, setflow / 100, actuatorposition, actuator, pressureconstant / 10, wateractuatordirection, flowtype,flowsetting, tstobuilding / 100, buildingtots / 100, coolingenergy / 1000, tsinlet / 100, tsoutlet / 100, tslinepressure / 100, tsstoredwatertemperature/100, vfdcurrent/100, vfdvoltage/100, cummulativeflow/100, flowvalueaverage, overallvaluesaverage, linepressurelowerlimit/100, linepresureupperlimit/100, setflowtolerance/100, linepressurelowerlimittolerance/100, linepressureupperlimittolerance/100, frequencypressurepidconstant/100, frequencyflowpidconstant/100, valvepressurepidconstant/100, valveflowpidconstant/100, allowablefrequency, cutofftemp/100, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, hvalve, hvalvefeedback, vfdalarm, kickofftempenable, cutofftemp_enable,maxvalueposition,errorcode,kitchofftemps/100,valopen,valclose)
      cursorObject.execute(mysql_insert_query, record_to_insert)
      mydb.commit()
    except Exception as error:
      print("the error occur during inserting data")
      logger.error(error)
  

       
       



while True:
  try:
    client = mqtt.Client()
    client.username_pw_set(username="swadha",password="dhawas@123")
    client.connect("10.9.39.25",1883,60)
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