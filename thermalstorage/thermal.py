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
		#current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
		if(data_dict["VALOPEN"]==0):
			valopen = "OFF"
		elif(data_dict["VALOPEN"]==1):
			valopen = "ON"
		if(data_dict["VALCLOSE"]==0):
			valclose = "OFF"
		elif(data_dict["VALCLOSE"]==1):
			valclose = "ON"

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
			mysql_insert_query = "INSERT INTO EMSThermalstorage (thermaldatetime, mode, vfdstatues, maxfrequency, minfrequency, frequency, setflow, actutatorposition, actutator, pressureconstant, wateractutator_direction, flowmetertype,flowsetting, thermalstorage_to_buildingflowrate, building_to_thermalstorage_flowrate, coolingenergy_consumption, thermalstorage_inlet, thermalstorage_outlet, thermalstorage_linepressure, thermalstorage_storedwatertemperature, vfdcurrent, vfdvoltage, cummulativeflow, flowvalve_average, overallvalve_average, linepressure_lowerlimit, linepressure_upperlimit, setflow_tolerence, linepressure_lowerlimittolerence,linepresssure_upperlimittolerence, frequencypressure_pidconstant, frequencyflow_pidconstant, valvepressure_pidconstant, valveflow_pidconstant, allowable_frequency, cutofftemp, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, H_valve, H_valvefeedback, vfdalarm,kichofftemp_enableORdisable, cutofftemp_enableordisable,maximumvalveposition,Error_at,kitchoff_temp,valve_open,valve_close) VALUES (%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
			record_to_insert = (date, mode, vfd, maxf / 100, minf / 100, frequency / 100, setflow / 100, actuatorposition, actuator, pressureconstant / 10, wateractuatordirection, flowtype,flowsetting, tstobuilding / 100, buildingtots / 100, coolingenergy / 1000, tsinlet / 100, tsoutlet / 100, tslinepressure / 100, tsstoredwatertemperature/100, vfdcurrent/100, vfdvoltage/100, cummulativeflow/100, flowvalueaverage, overallvaluesaverage, linepressurelowerlimit/100, linepresureupperlimit/100, setflowtolerance/100, linepressurelowerlimittolerance/100, linepressureupperlimittolerance/100, frequencypressurepidconstant/100, frequencyflowpidconstant/100, valvepressurepidconstant/100, valveflowpidconstant/100, allowablefrequency, cutofftemp/100, bdpvalve, bdpvalvefeedback, adpvalve, adpvalvefeedback, hvalve, hvalvefeedback, vfdalarm, kickofftempenable, cutofftemp_enable,maxvalueposition,error_type,kitchofftemps/100,valopen,valclose)
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
