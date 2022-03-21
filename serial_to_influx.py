from datetime import datetime
import json
import os
from optparse import OptionParser

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import requests

import time,re
import os
import sys
import logging
import logging.handlers
import serial

def reset_db():
    influxDBHost = os.getenv("INFLUX_HOST")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("ORG")
    bucket = os.getenv("BUCKET")
    measurement = os.getenv("MEASUREMENT")
    
    with InfluxDBClient(url=influxDBHost, token=token, org=org) as client:
        start = "1970-01-01T00:00:00Z"
        stop = datetime.utcnow()
        client.delete_api().drop_measurement(start, stop,'_measurement="'+measurement+'"', bucket, org)

##----------------------------- Influx connect finish
def dataformat(data):
    try:
        ID = re.findall(r",ID:(.+?),STAT",data)
        TEMP = re.findall(r",T:(.+?)\\xa1\\xe6,", data)
        HUMIDITY = re.findall(r",H:(.+?)%,", data)
        CURRENTDT = str(datetime.now())
        return {"mac_add": ID[0], "time": CURRENTDT, "temperature": getDefaultReading(TEMP), "humidity": getDefaultReading(HUMIDITY)}
    except ValueError as e:
        logging.error("Data Formate is wrong:" + str(e))

def getDefaultReading(reading):
    retVal = None
    if len(reading):
        retVal = reading[0]
    return retVal

def store_reading_influx(value):
    '''insert measurements from serial sensor to the db.
    value is formatted value from sensor received here
    '''
    influxDBHost = os.getenv("INFLUX_HOST")
    token = os.getenv("INFLUX_TOKEN")
    org = os.getenv("ORG")
    bucket = os.getenv("BUCKET")
    measurement = os.getenv("MEASUREMENT")
    
    sensorName,sensorZone, sensorCluster = get_sensor_name(value["mac_add"])

    with InfluxDBClient(url=influxDBHost, token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        point = Point(measurement) \
        .tag("mac", value["mac_add"]) \
        .tag("sensorName", sensorName) \
        .tag("zone", sensorZone) \
        .tag("cluster", sensorCluster) \
        .field("temperature", value['temperature']) \
        .field("humidity", value['humidity']) \
        .time(datetime.utcnow(), WritePrecision.NS)
        write_api.write(bucket, org, point)
        client.close()
    time.sleep(1)

def get_sensor_name(sensorID):
    sensorDetails = os.getenv("SENSORS")
    name = "other"
    zone = "default"
    cluster = "default"
    sensors = json.loads(sensorDetails)
    sensor = sensors.get(sensorID)
    if sensor :
        name = sensor["name"]
        zone = sensor["zone"]
        cluster = sensor["cluster"]
    
    return name, zone, cluster
        


if __name__ == '__main__':
    load_dotenv()
    parser = OptionParser('%prog [OPTIONS]')
    parser.add_option(
        '-r', '--reset', dest='reset',
        help='reset database',
        default=False,
        action='store_true'
        )
   
    options, args = parser.parse_args()
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

    if (options.reset):
        reset_db()

    try:
        serialPort = os.getenv("SERIAL_PORT")
        ser = serial.Serial(serialPort, baudrate=9600, timeout=3.0)
    except Exception as e:
        logging.error("Port is not Working:" + str(e))
    while True:
        try:
            rcv = ser.read(300)
            # following is a sample for testing
            # rcv = '"GW_ID:2,TYPE:T&H,ID:11200006,STAT:00000000,T:25.9\xa1\xe6,H:62.3%,ST:5M,V:3.55v,SN:72,RSSI:-48dBm,S:22.5769,E:113.9712,Time:0-0-0 0:0:0,T_RSSI:-80dBm\r\n"'
            if (str(rcv)[1:] != '\'\''):
                data = str(rcv)[1:].replace('\'','')
                logging.debug("data: "+data)
                try:
                    value = dataformat(data)
                    try:
                        store_reading_influx(value)
                    except Exception as e:
                        logging.exception("Failed in writing: " + str(value))
                except Exception as e:
                    logging.error("record value:" + data)
                    logging.error("record error:" + str(e))
        except:
            try:
                time.sleep(10)
                ser = serial.Serial(serialPort, baudrate=9600, timeout=3.0)
            except:
                logging.error("Waiting for gateway.")
                break