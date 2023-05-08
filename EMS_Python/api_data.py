import requests
import json
import datetime,time
from time import sleep
import mysql.connector
from datetime import datetime
import tzlocal

import Class_Definition

conn = None
conn = mysql.connector.connect(host='localhost',database='EMS',user='root',password='22@teneT')
cursor = conn.cursor()                                     
                                                                            
    
while(1) :

    endTime =  int(time.mktime(datetime.now().timetuple()) * 1000 + datetime.now().microsecond/1000)   #1673432769382 #
    startTime = endTime - 60000  #1min = 60000 milliseconds  #1673432656462 
    frequency = 1

    url = Class_Definition.Config.config['connection']['url']
    url = url.format(startTime,endTime,frequency) 
    
    status= ""
    
    response_API = requests.get(url, headers = {"X-API-KEY": Class_Definition.Config.config['connection']['X-API-KEY']})
    if(response_API):
      response_timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
      status = "Success"
      
    else:
      status = "failed"
      
    data = response_API.text
    response = json.loads(data)  #json data
    print(response)
    
    #REQRES_TIMESTAMP
    request_timestamp = response['data']['requesttimestamp']/1000
    local_timezone = tzlocal.get_localzone() # get pytz timezone
    request_timestamp_local_time = datetime.fromtimestamp(request_timestamp, local_timezone).strftime("%Y-%m-%d %H:%M:%S")

    
    insert_query = """ INSERT INTO EMSResTimestamp (reqtimestamp,restimestamp,reqresrequest,reqresresponse,reqresstatus) VALUES (%s,%s,%s,%s,%s)"""
    record_to_insert = (request_timestamp_local_time,response_timestamp,url,str(response),status)
    cursor.execute(insert_query, record_to_insert)
    conn.commit() 
    
    #WMS_TYPES
    wmsMeta_type_id = response['data']['meta']['wms'][0]['wmstypeid']
    wmsMeta_count = response['data']['meta']['wms'][0]['wmscount']
    wmsMeta_type = response['data']['meta']['wms'][0]['wmstype']
    wmsMeta_make = response['data']['meta']['wms'][0]['wmsmake']
    wmsMeta_model = response['data']['meta']['wms'][0]['wmsmodel']
    
    cursor.execute("""SELECT wmstypeid FROM EMSWMSTypes""")
    type_id_wms = cursor.fetchall()
    
    if(type_id_wms == []) :     #adding first type id value
         insert_query = """ INSERT INTO EMSWMSTypes (wmstypeid,wmscount,wmstype,wmsmake,wmsmodel) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (wmsMeta_type_id,wmsMeta_count,wmsMeta_type,wmsMeta_make,wmsMeta_model)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
    
    for i in type_id_wms:
      if(wmsMeta_type_id not in i) :  #if wmsMeta_type_id is not in db then add 
        
         insert_query = """ INSERT INTO EMSWMSTypes (wmstypeid,wmscount,wmstype,wmsmake,wmsmodel) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (wmsMeta_type_id,wmsMeta_count,wmsMeta_type,wmsMeta_make,wmsMeta_model)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
    
    #INVERTER_TYPES
    inverterMeta_type_id = response['data']['meta']['inverters'][0]['invertertypeid']
    inverterMeta_count = response['data']['meta']['inverters'][0]['invertercount']
    inverterMeta_string_inverterCount = response['data']['meta']['inverters'][0]['stringinvertercount']
    inverterMeta_type = response['data']['meta']['inverters'][0]['invertertype']
    inverterMeta_inverter_make = response['data']['meta']['inverters'][0]['invertermake']
    inverterMeta_inverter_model = response['data']['meta']['inverters'][0]['invertermodel']
    inverterMeta_capacity = response['data']['meta']['inverters'][0]['plant capacity']
    inverterMeta_panelMake = response['data']['meta']['inverters'][0]['panelmake']
    inverterMeta_panelEfficiency = response['data']['meta']['inverters'][0]['panelefficency']
    
    cursor.execute("select invertertypeid from EMSInverterTypes")
    type_id_inverter = cursor.fetchall()
    
    if(type_id_inverter == []) :
        insert_query = """ INSERT INTO EMSInverterTypes (invertertypeid,invertercount,stringinvertercount,invertertype,invertermake,invertermodel,invertercapacity,inverterpanelmake,inverterpanelefficency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        record_to_insert = (inverterMeta_type_id,inverterMeta_count,inverterMeta_string_inverterCount,inverterMeta_type,inverterMeta_inverter_make,inverterMeta_inverter_model,inverterMeta_capacity,inverterMeta_panelMake,inverterMeta_panelEfficiency)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()
        
    for i in type_id_inverter :
        if(inverterMeta_type_id not in i) :
           insert_query = """ INSERT INTO EMSInverterTypes (invertertypeid,invertercount,stringinvertercount,invertertype,invertermake,invertermodel,invertercapacity,inverterpanelmake,inverterpanelefficency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
           record_to_insert = (inverterMeta_type_id,inverterMeta_count,inverterMeta_string_inverterCount,inverterMeta_type,inverterMeta_inverter_make,inverterMeta_inverter_model,inverterMeta_capacity,inverterMeta_panelMake,inverterMeta_panelEfficiency)
           cursor.execute(insert_query, record_to_insert)
           conn.commit()
    
    #METER_TYPES
    meterMeta_type_id = response['data']['meta']['meters'][0]['metertypeid']
    meterMeta_count = response['data']['meta']['meters'][0]['metercount']
    meterMeta_type = response['data']['meta']['meters'][0]['metertype']
    meterMeta_make = response['data']['meta']['meters'][0]['metermake']
    meterMeta_model = response['data']['meta']['meters'][0]['metermodel']
    
    cursor.execute("select metertypeid from EMSMeterTypes")
    type_id_meter = cursor.fetchall()
    
    if(type_id_meter==[]):
         insert_query = """ INSERT INTO EMSMeterTypes (metertypeid,metercount,metertype,metermake,metermodel) VALUES (%s,%s,%s,%s,%s)"""
         record_to_insert = (meterMeta_type_id,meterMeta_count,meterMeta_type,meterMeta_make,meterMeta_model)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
         
    for i in type_id_meter :
         if(meterMeta_type_id not in  i) :
    
            insert_query = """ INSERT INTO EMSMeterTypes (metertypeid,metercount,metertype,metermake,metermodel) VALUES (%s,%s,%s,%s,%s)"""
            record_to_insert = (meterMeta_type_id,meterMeta_count,meterMeta_type,meterMeta_make,meterMeta_model)
            cursor.execute(insert_query, record_to_insert)
            conn.commit()
    

         
    for i in device_id_wms:    
         if(wms_inst_device_id not in  i) :
    
            insert_query = """ INSERT INTO EMSWMSInstances (wmstypeid,wmsname,wmsdeviceid) VALUES (%s,%s,%s)"""
            record_to_insert = (wms_inst_type_id,wms_inst_name,wms_inst_device_id)
            cursor.execute(insert_query, record_to_insert)
            conn.commit()
    
    #INVERTER_INSTANCES
    inverter_inst_type_id = inverterMeta_type_id
    for i in range(inverterMeta_string_inverterCount) :
        inverter_inst_name = response['data']['data'][0]['inverter'][i]['name']
        inverter_inst_device_id = response['data']['data'][0]['inverter'][i]['deviceid']
        
        cursor.execute("select inverterdeviceid from  EMSInverterInstances")
        device_id_inverter = cursor.fetchall()
        
        if(device_id_inverter == []) :
           insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
           record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
           cursor.execute(insert_query, record_to_insert)
           conn.commit()
    
    
        if(inverter_inst_device_id not in device_id_inverter[i]) :
           insert_query = """ INSERT INTO EMSInverterInstances (invertertypeid,invertername,inverterdeviceid) VALUES (%s,%s,%s)"""
           record_to_insert = (inverter_inst_type_id,inverter_inst_name,inverter_inst_device_id)
           cursor.execute(insert_query, record_to_insert)
           conn.commit()
        
        
    #METER_INSTANCES
    meter_inst_type_id =  meterMeta_type_id
    meter_inst_name = response['data']['data'][0]['meter'][0]['name'] 
    meter_inst_device_id = response['data']['data'][0]['meter'][0]['deviceid'] 
    
    cursor.execute("select meterdeviceid from  EMSMeterInstances")
    device_id_meter = cursor.fetchall()
    
    if(device_id_meter == []) :
        insert_query = """ INSERT INTO EMSMeterInstances (metertypeid,metername,meterdeviceid) VALUES (%s,%s,%s)"""
        record_to_insert = (meter_inst_type_id,meter_inst_name,meter_inst_device_id)
        cursor.execute(insert_query, record_to_insert)
        conn.commit()
    for i in device_id_meter : 
        if(meter_inst_device_id not in i) :
    
           insert_query = """ INSERT INTO EMSMeterInstances (metertypeid,metername,meterdeviceid) VALUES (%s,%s,%s)"""
           record_to_insert = (meter_inst_type_id,meter_inst_name,meter_inst_device_id)
           cursor.execute(insert_query, record_to_insert)
           conn.commit()
    
    #WMS_DATA
    wms_device_id = response['data']['data'][0]['wms'][0]['deviceid']
    wms_type_id = response['data']['data'][0]['wms'][0]['devicetypeid']
    wms_name = response['data']['data'][0]['wms'][0]['name']
    wms_ambient_temp = response['data']['data'][0]['wms'][0]['ambienttemprature']
    wms_irradiation =  response['data']['data'][0]['wms'][0]['Irradiation'] 
    wms_humidity = response['data']['data'][0]['wms'][0]['humidity'] 
    wms_wind_speed = response['data']['data'][0]['wms'][0]['windspeed'] 
    
    insert_query = """ INSERT INTO EMSWMSData (wmstimestamp,wmsdeviceid,wmstypeid,wmsname,wmsambienttemp,wmsirradiation,wmshumidity,wmswindspeed) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),wms_device_id,wms_type_id,wms_name,wms_ambient_temp,wms_irradiation,wms_humidity,wms_wind_speed)
    cursor.execute(insert_query, record_to_insert)
    conn.commit()
    
    #INVERTER_DATA
    for i in range(inverterMeta_string_inverterCount) :
         inverter_device_id = response['data']['data'][0]['inverter'][i]['deviceid']
         inverter_type_id = response['data']['data'][0]['inverter'][i]['devicetypeid'] 
         inverter_name = response['data']['data'][0]['inverter'][i]['name'] 
         inverter_energy = response['data']['data'][0]['inverter'][i]['energy'] 
         inverter_active_power = response['data']['data'][0]['inverter'][i]['activepower']
         inverter_frequency = response['data']['data'][0]['inverter'][i]['frequency']
         inverter_reactive_power = response['data']['data'][0]['inverter'][i]['reactivepower']
         inverter_dc_power = response['data']['data'][0]['inverter'][i]['dcpower']
         inverter_temperature = response['data']['data'][0]['inverter'][i]['invertertemparturei']
         inverter_status =  response['data']['data'][0]['inverter'][i]['status']
         inverter_power_setpoint = response['data']['data'][0]['inverter'][i]['powersetpoint']
         inverter_dc_current = response['data']['data'][0]['inverter'][i]['dccurrent']
         inverter_dc_voltage = response['data']['data'][0]['inverter'][i]['dcvoltage']

         insert_query = """ INSERT INTO EMSInverterData (invertertimestamp,inverterdeviceid,inverterdevicetypeid,invertername,inverterEnergy,inverteractivepower,inverterfrequency,inverterreactivepower,inverterdcpower,invertertemperature,inverterstatus,inverterpowersetpoints,inverterdccurrent,inverterdcvoltage) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
         record_to_insert = (datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),inverter_device_id,inverter_type_id,inverter_name,inverter_energy,inverter_active_power,inverter_frequency,inverter_reactive_power,inverter_dc_power,inverter_temperature,inverter_status,inverter_power_setpoint,inverter_dc_current,inverter_dc_voltage)
         cursor.execute(insert_query, record_to_insert)
         conn.commit()
         
    #METER_DATA
    meter_device_id = response['data']['data'][0]['meter'][0]['deviceid'] 
    meter_type_id = response['data']['data'][0]['meter'][0]['devicetypeid']
    meter_name = response['data']['data'][0]['meter'][0]['name'] 
    meter_energy = response['data']['data'][0]['meter'][0]['energy']
    meter_power = response['data']['data'][0]['meter'][0]['power']
  
    insert_query = """ INSERT INTO EMSMeterData (metertimestamp,meterdeviceid,metertypeid,metername,meterenergy,meterpower) VALUES (%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),meter_device_id,meter_type_id,meter_name,meter_energy,meter_power)
    cursor.execute(insert_query, record_to_insert)
    conn.commit()
    
   
    
    time.sleep(300)