import paho.mqtt.client as mqtt
import mysql.connector
import datetime
import json
import logging

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


def convert_two_byte_to_int(first_byte,second_byte):
     return (first_byte << 16) + second_byte

def convert_four_byte_to_int(first_byte,second_byte,third_byte,fourth_byte):
     list =   [first_byte,second_byte,third_byte,fourth_byte]
     # Converting integer list to string list
     s = [str(i) for i in list]
     # Join list items using join()
     res = int("".join(s))    
     return res



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
		if(data_dict["MOD"]== 0):
			mode = "AUTO"
		elif(data_dict["MOD"]== 1):
			mode = "MANUAL"
		if(data_dict["VFS"] == 0):
			vfd = "OFF"
		elif(data_dict["VFS"] == 1):
			vfd = " ON"
		mxs_bytes = (data_dict["MXF"] >> 24 & 0xFF, data_dict["MXF"] >> 16 & 0xFF, data_dict["MXF"] >> 8 & 0xFF, data_dict["MXF"] & 0xFF)
		minf = convert_four_byte_to_int(*mxs_bytes)
		mxs_bytes = (data_dict["MNF"] >> 24 & 0xFF, data_dict["MNF"] >> 16 & 0xFF, data_dict["MNF"] >> 8 & 0xFF, data_dict["MNF"] & 0xFF)
		maxf = convert_four_byte_to_int(*mxs_bytes)
		frequency = data_dict["FRQ"]
		setflow = data_dict["SFW"]
		actuatorposition = data_dict["WAP"]
		if(data_dict["ANF"] == 0):
			actuator = "OFF"
		elif(data_dict["ANF"] == 1):
			actuator = "ON"
		pressureconstant = data_dict["PCT"]
		if(data_dict["WAD"]== 0):
			wateractuatordirection = "FORWARD"
		elif(data_dict["WAD"] == 1):
			wateractuatordirection = "REVERSE"
		if(data_dict["FLT"]== 0):
			flowtype = "CURRENTFLOW(4-20mA)"
		elif(data_dict["FLT"] == 1):
			flowtype = "VOLTAGEFLOW(2_10V)"
		elif(data_dict["FLT"] == 2):
			flowtype = "VOLTAGEFLOW(0.5-10V)"
		if data_dict["FLT"] == 0:
			if (data_dict["FTP"]) == 0:
				flowsetting = "flowspan_50"
			elif(data_dict["FTP"]) == 1:
				flowsetting = "flowspan_90"
		elif data_dict["FLT"] == 1:
			if(data_dict["FTP"]) == 0:
				flowsetting = "PIPESIZE_25MM"
			elif(data_dict["FTP"] == 1):
				flowsetting = "PIPESIZE_32MM"
			elif(data_dict["FTP"] == 2):
				flowsetting = "PIPESIZE_40MM"
			elif(data_dict["FTP"] == 3):
				flowsetting = "PIPESIZE_50MM"
			elif(data_dict["FTP"] == 4):
				flowsetting = "PIPESIZE_50A"
			elif(data_dict["FTP"] == 5):
				flowsetting = "EP065F+MP"
			elif(data_dict["FTP"] == 6):
				flowsetting = "EP080F+MP"
			elif(data_dict["FTP"] == 7):
				flowsetting = "EP100F+MP"
			elif(data_dict["FTP"] == 8):
				flowsetting = "EP125F+MP"
			elif(data_dict["FTP"] == 9):
				flowsetting = "EP150F+MP"
			else:
				flowsetting = "not in range"
		elif data_dict["FLT"] == 2:
			if(data_dict["FTP"]) == 0:
				flowsetting = "FM_15R"
			elif(data_dict["FTP"] == 1):
				flowsetting = "FM_20R"
			elif(data_dict["FTP"] == 2):
				flowsetting = "FM_25R"
			elif(data_dict["FTP"] == 3):
				flowsetting = "FM_32R"
			elif(data_dict["FTP"] == 4):
				flowsetting = "FM_40R"
			elif(data_dict["FTP"] == 5):
				flowsetting = "FM_50R"
			elif(data_dict["FTP"] == 6):
				flowsetting = "FM_65F"
			elif(data_dict["FTP"] == 7):
				flowsetting = "FM_80F"
			elif(data_dict["FTP"] == 8):
				flowsetting = "FM_100F"
			elif(data_dict["FTP"] == 9):
				flowsetting = "FM_125F"
			elif(data_dict["FTP"] == 10):
				flowsetting = "FM_150F"

			else:
				flowsetting = "not in range"


		


		tstobuilding = data_dict["DFLR"]
		buildingtots = data_dict["ACFLR"]
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
		cutofftemp= data_dict["COT"]
		if(data_dict["BDPV"]== 0):
			bdpvalve = "OFF"
		elif(data_dict["BDPV"]==1):
			bdpvalve = "ON"
		if(data_dict["BDPVF"]==0):
			bdpvalvefeedback = "OFF"
		elif(data_dict["BDPVF"]==1):
			bdpvalvefeedback ="ON"
		if(data_dict["ADPV"]==0):
			adpvalve = "OFF"
		elif(data_dict["ADPV"]==1):
			adpvalve = "ON"
		if(data_dict["ADPVF"]==0):
			adpvalvefeedback= "OFF"
		elif(data_dict["ADPVF"]==1):
			adpvalvefeedback= "ON"
		if(data_dict["HV"]==0):
			hvalve = "OFF"
		elif(data_dict["HV"]==1):
			hvalve = "ON"
		if(data_dict["HVF"]==0):
			hvalvefeedback =  "OFF"
		elif(data_dict["HVF"]==1):
			hvalvefeedback= "0N"
		vfdalarm =data_dict["VFDA"]
		if(data_dict["KOTED"]==0):
			kickofftempenable ="enable"
		elif(data_dict["KOTED"]==1):
			kickofftempenable = "disable"
		kickofftemp = data_dict["KOT"]
		if(data_dict["COTED"]==0):
			cutofftemp_enable= "enable"
		elif(data_dict["COTED"]==1):
			cutofftemp_enable = "disable"
		maxvalueposition = data_dict["MVP"]
		errorcode = data_dict["ERROR"]
		binary_num = format(errorcode, '032b')
		if binary_num[31] == '1':
			error_type = "mode"
		elif binary_num[30] == "1":
			error_type = "vfd status"
		elif binary_num[29] == "1":
			error_type == "maxfrequency"
		elif binary_num[28] == "1":
			error_type ="min frequency"
		elif binary_num[27] == "1":
			error_type = "frequency"
		elif binary_num[26] == "1":
			error_type ="set charging flowtype"
		elif binary_num[25] == "1":
			error_type = "acuator position"
		elif binary_num[24] == "1":
			error_type ="set max flow"
		elif binary_num[23] == "1":
			error_type ="epiv on/off"
		elif binary_num[22] == "1":
			error_type ="pid constant"
		elif binary_num[21] == "1":
			error_type ="water acuator position"
		else:
			error_type = "no errors"
			print("Error type: " + error_type)

		try:
			mysql_insert_query = "INSERT INTO EMSThermalstorage (thermaldatetime, mode, vfdstatues, maxfrequency, minfrequency, frequency, setflow, actutatorposition, actutator, pressureconstant, wateractutator_direction, flowmetertype,flowsetting, thermalstorage_to_buildingflowrate, building_to_thermalstorage_flowrate, coolingenergy_consumption, thermalstorage_inlet, thermalstorage_outlet, thermalstorage_linepressure, thermalstorage_storedwatertemperature, vfdcurrent, vfdvoltage, cummulativeflow, flowvalve_average, overallvalve_average, linepressure_lowerlimit, linepressure_upperlimit, setflow_tolerence, linepressure_lowerlimittolerence,linepresssure_upperlimittolerence, frequencypressure_pidconstant, frequencyflow_pidconstant, valuepressure_pidconstant, valueflow_pidconstant, allowable_frequency, cutofftemp, bdpvalue, bdpvaluefeedback, adpvalue, adpvaluefeedback, H_value, H_valuefeedback, vfdalarm,kichofftemp, cutofftemp_enableordisable,maximumvalueposition,caution_at) VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
			record_to_insert = (date, mode, vfd, maxf / 100, minf / 100, frequency / 100, setflow / 10, actuatorposition, actuator, pressureconstant / 10, wateractuatordirection, flowtype,flowsetting, tstobuilding / 100, buildingtots / 100, coolingenergy / 100, tsinlet / 100, tsoutlet / 100, tslinepressure / 100, tsstoredwatertemperature/100, vfdcurrent/100, vfdvoltage/100, cummulativeflow/100, flowvalueaverage, overallvaluesaverage, linepressurelowerlimit, linepresureupperlimit, setflowtolerance, linepressurelowerlimittolerance, linepressureupperlimittolerance, frequencypressurepidconstant, frequencyflowpidconstant, valvepressurepidconstant, valveflowpidconstant, allowablefrequency, cutofftemp, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, hvalve, hvalvefeedback, vfdalarm, kickofftempenable, cutofftemp_enable,maxvalueposition,error_type)
			cursorObject.execute(mysql_insert_query, record_to_insert)
			mydb.commit()
		except Exception as error:
			print("the error occur during inserting data")
			logger.error(error)
	

       
       







while(1):
	client = mqtt.Client()
	client.username_pw_set(username="swadha",password="dhawas@123")
	client.connect("10.9.39.25",1883,60)
	client.on_connect = on_connect
	client.on_message = on_message
	client.loop_forever()//////


	import paho.mqtt.client as mqtt
import mysql.connector
import datetime
import json
import logging

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


def convert_two_byte_to_int(first_byte,second_byte):
     return (first_byte << 16) + second_byte

def convert_four_byte_to_int(first_byte,second_byte,third_byte,fourth_byte):
     list =   [first_byte,second_byte,third_byte,fourth_byte]
     # Converting integer list to string list
     s = [str(i) for i in list]
     # Join list items using join()
     res = int("".join(s))    
     return res



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
		if(data_dict["MOD"]== 0):
			mode = "AUTO"
		elif(data_dict["MOD"]== 1):
			mode = "MANUAL"
		if(data_dict["VFS"] == 0):
			vfd = "OFF"
		elif(data_dict["VFS"] == 1):
			vfd = " ON"
		minf = data_dict["MXF"]
		maxf = data_dict["MNF"]
		frequency = data_dict["FRQ"]
		setflow = data_dict["SFW"]
		actuatorposition = data_dict["WAP"]
		if(data_dict["ANF"] == 0):
			actuator = "OFF"
		elif(data_dict["ANF"] == 1):
			actuator = "ON"
		pressureconstant = data_dict["PCT"]
		if(data_dict["WAD"]== 0):
			wateractuatordirection = "FORWARD"
		elif(data_dict["WAD"] == 1):
			wateractuatordirection = "REVERSE"
		if(data_dict["FLT"]== 0):
			flowtype = "CURRENTFLOW(4-20mA)"
		elif(data_dict["FLT"] == 1):
			flowtype = "VOLTAGEFLOW(2_10V)"
		elif(data_dict["FLT"] == 2):
			flowtype = "VOLTAGEFLOW(0.5-10V)"
		if data_dict["FLT"] == 0:
			if (data_dict["FTP"]) == 0:
				flowsetting = "flowspan_50"
			elif(data_dict["FTP"]) == 1:
				flowsetting = "flowspan_90"
		elif data_dict["FLT"] == 1:
			if(data_dict["FTP"]) == 0:
				flowsetting = "PIPESIZE_25MM"
			elif(data_dict["FTP"] == 1):
				flowsetting = "PIPESIZE_32MM"
			elif(data_dict["FTP"] == 2):
				flowsetting = "PIPESIZE_40MM"
			elif(data_dict["FTP"] == 3):
				flowsetting = "PIPESIZE_50MM"
			elif(data_dict["FTP"] == 4):
				flowsetting = "PIPESIZE_50A"
			elif(data_dict["FTP"] == 5):
				flowsetting = "EP065F+MP"
			elif(data_dict["FTP"] == 6):
				flowsetting = "EP080F+MP"
			elif(data_dict["FTP"] == 7):
				flowsetting = "EP100F+MP"
			elif(data_dict["FTP"] == 8):
				flowsetting = "EP125F+MP"
			elif(data_dict["FTP"] == 9):
				flowsetting = "EP150F+MP"
			else:
				flowsetting = "not in range"
		elif data_dict["FLT"] == 2:
			if(data_dict["FTP"]) == 0:
				flowsetting = "FM_15R"
			elif(data_dict["FTP"] == 1):
				flowsetting = "FM_20R"
			elif(data_dict["FTP"] == 2):
				flowsetting = "FM_25R"
			elif(data_dict["FTP"] == 3):
				flowsetting = "FM_32R"
			elif(data_dict["FTP"] == 4):
				flowsetting = "FM_40R"
			elif(data_dict["FTP"] == 5):
				flowsetting = "FM_50R"
			elif(data_dict["FTP"] == 6):
				flowsetting = "FM_65F"
			elif(data_dict["FTP"] == 7):
				flowsetting = "FM_80F"
			elif(data_dict["FTP"] == 8):
				flowsetting = "FM_100F"
			elif(data_dict["FTP"] == 9):
				flowsetting = "FM_125F"
			elif(data_dict["FTP"] == 10):
				flowsetting = "FM_150F"

			else:
				flowsetting = "not in range"


		


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
		cutofftemp= data_dict["COT"]
		if(data_dict["BDPV"]== 0):
			bdpvalve = "OFF"
		elif(data_dict["BDPV"]==1):
			bdpvalve = "ON"
		if(data_dict["BDPVF"]==0):
			bdpvalvefeedback = "OFF"
		elif(data_dict["BDPVF"]==1):
			bdpvalvefeedback ="ON"
		if(data_dict["ADPV"]==0):
			adpvalve = "OFF"
		elif(data_dict["ADPV"]==1):
			adpvalve = "ON"
		if(data_dict["ADPVF"]==0):
			adpvalvefeedback= "OFF"
		elif(data_dict["ADPVF"]==1):
			adpvalvefeedback= "ON"
		if(data_dict["HV"]==0):
			hvalve = "OFF"
		elif(data_dict["HV"]==1):
			hvalve = "ON"
		if(data_dict["HVF"]==0):
			hvalvefeedback =  "OFF"
		elif(data_dict["HVF"]==1):
			hvalvefeedback= "0N"
		vfdalarm =data_dict["VFDA"]
		if(data_dict["KOTED"]==0):
			kickofftempenable ="DISABLE"
		elif(data_dict["KOTED"]==1):
			kickofftempenable = "ENABLE"
		kickofftemp = data_dict["KOT"]
		if(data_dict["COTED"]==0):
			cutofftemp_enable= "DISABLE"
		elif(data_dict["COTED"]==1):
			cutofftemp_enable = "ENABLE"
		maxvalueposition = data_dict["MVP"]
		errorcode = data_dict["ERROR"]
		binary_num = format(errorcode, '032b')
		if binary_num[31] == '1':
			error_type = "mode"
		elif binary_num[30] == "1":
			error_type = "vfd status"
		elif binary_num[29] == "1":
			error_type == "maxfrequency"
		elif binary_num[28] == "1":
			error_type ="min frequency"
		elif binary_num[27] == "1":
			error_type = "frequency"
		elif binary_num[26] == "1":
			error_type ="set charging flowtype"
		elif binary_num[25] == "1":
			error_type = "acuator position"
		elif binary_num[24] == "1":
			error_type ="set max flow"
		elif binary_num[23] == "1":
			error_type ="epiv on/off"
		elif binary_num[22] == "1":
			error_type ="pid constant"
		elif binary_num[21] == "1":
			error_type ="water acuator position"
		else:
			error_type = "no errors"
			print("Error type: " + error_type)

		try:
			mysql_insert_query = "INSERT INTO EMSThermalstorage (thermaldatetime, mode, vfdstatues, maxfrequency, minfrequency, frequency, setflow, actutatorposition, actutator, pressureconstant, wateractutator_direction, flowmetertype,flowsetting, thermalstorage_to_buildingflowrate, building_to_thermalstorage_flowrate, coolingenergy_consumption, thermalstorage_inlet, thermalstorage_outlet, thermalstorage_linepressure, thermalstorage_storedwatertemperature, vfdcurrent, vfdvoltage, cummulativeflow, flowvalve_average, overallvalve_average, linepressure_lowerlimit, linepressure_upperlimit, setflow_tolerence, linepressure_lowerlimittolerence,linepresssure_upperlimittolerence, frequencypressure_pidconstant, frequencyflow_pidconstant, valvepressure_pidconstant, valveflow_pidconstant, allowable_frequency, cutofftemp, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, H_valve, H_valvefeedback, vfdalarm,kichofftemp_enableORdisable, cutofftemp_enableordisable,maximumvalveposition,caution_at,kitchoff_temp) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
			record_to_insert = (date, mode, vfd, maxf / 100, minf / 100, frequency / 100, setflow / 100, actuatorposition, actuator, pressureconstant / 10, wateractuatordirection, flowtype,flowsetting, tstobuilding / 100, buildingtots / 100, coolingenergy / 1000, tsinlet / 100, tsoutlet / 100, tslinepressure / 100, tsstoredwatertemperature/100, vfdcurrent/100, vfdvoltage/100, cummulativeflow/100, flowvalueaverage, overallvaluesaverage, linepressurelowerlimit/100, linepresureupperlimit/100, setflowtolerance/100, linepressurelowerlimittolerance/100, linepressureupperlimittolerance/100, frequencypressurepidconstant/100, frequencyflowpidconstant/100, valvepressurepidconstant/100, valveflowpidconstant/100, allowablefrequency, cutofftemp/100, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, hvalve, hvalvefeedback, vfdalarm, kickofftempenable, cutofftemp_enable,maxvalueposition,error_type,kitchofftemps/100)
			cursorObject.execute(mysql_insert_query, record_to_insert)
			mydb.commit()
		except Exception as error:
			print("the error occur during inserting data")
			logger.error(error)
	

       
       







while(1):
	client = mqtt.Client()
	client.username_pw_set(username="swadha",password="dhawas@123")
	client.connect("10.9.39.25",1883,60)
	client.on_connect = on_connect
	client.on_message = on_message
	client.loop_forever()///////



	import paho.mqtt.client as mqtt
from datetime import datetime
import tzlocal
import mysql.connector
import ast
import logging             #bytes to dict

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
        apparantpowerzphase = data1["VAB"]
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
        insert_query = """ INSERT INTO EMSUPSLoad (loadtimestamp,totalpower,power_R_phase,power_Y_phase,power_B_phase,powerfactoravarage,powerfactor_R_phase,powerfactor_Y_phase,powerfactor_B_phase,totalapparantpower,apparantpower_R_phase,apparantpower_Y_phase,apparantpower_B_phase,voltagelinetolineaverage,voltage_RY_phase,voltage_YB_phase,voltage_BR_phase,voltagelinenuetralaverage,voltage_R_phase,voltage_Y_phase,voltage_B_phase,currenttotal,current_R_phase,current_Y_phase,current_B_phase,frequency,activeenergy_EB,apparantenergy_EB,activeenergy_DG,apparantenergy_DG,loadhours_EB,loadhours_DG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (timestamp_local,totalpower,powerrphase,poweryphase,powerbphase,powerfactoravarage,powerfactorrphase,powerfactoryphase,powerfactorrphase,totalapperantpower,apparantpowerrphase,apparantpoweryphase,apparantpowerzphase,voltagelinetolineavarage,voltageryphase,voltageybphase,voltagebrphase,voltagelineneutralavaerage,voltagerphase,voltageyphase,voltagebphase,currenttotal,currentrphase,currentyphase,currentbphase,frequency,activeenergyeb,apparantenergyeb,activeenergydg,apparantenergydg,loadhourseb,loadhoursdb)
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
             
         charge_energy = data1["BCHE"]
         discharge_energy = data1["BDCHE"]
         avail_energy_per = data1["BUSOC"]
         
         timestamp_unix = data1['BRTC']    #converting from unix timestamp to local time
         local_timezone = tzlocal.get_localzone() # get pytz timezone
         timestamp_local = datetime.fromtimestamp(timestamp_unix, local_timezone).strftime("%Y-%m-%d %H:%M:%S") #from unix to localtime
         #print(timestamp_local,178) 
         
         #inserting into database
         insert_query = """ INSERT INTO EMSUPSbattery (upstimestamp,upschargingenergy,upsdischargingenergy,upsenergyavailablepercentage,upsbatterystatus) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (timestamp_local,charge_energy,discharge_energy,avail_energy_per,battery_status)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()


    if message.topic == Class_Definition.Config.config['Topics']['Charger'] :
         print(data1)
         
    if message.topic == Class_Definition.Config.config['Topics']['CV'] :
         print(data1)
         
    if message.topic == Class_Definition.Config.config['Topics']['CT'] :
         print(data1)
         
    if message.topic == Class_Definition.Config.config['Topics']['Specs'] :
         print(data1)
         
while(1):
    client = mqtt.Client()
    #client.username_pw_set(username="admin",password="admin@123")
    #client.connect("10.9.211.140",1883,60)
    client.username_pw_set(username=Class_Definition.Config.config['Broker_connection']['userName'],password=Class_Definition.Config.config['Broker_connection']['password'])
    client.connect(Class_Definition.Config.config['Broker_connection']['broker'],Class_Definition.Config.config['Broker_connection']['port'], Class_Definition.Config.config['Broker_connection']['keep_alive'])
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()
