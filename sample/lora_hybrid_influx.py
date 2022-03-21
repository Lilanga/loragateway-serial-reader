from influxdb import InfluxDBClient
import requests

import time,re
import datetime
import math
import pprint
import os
import signal
import sys
import logging
import logging.handlers
import serial


client = None
dbname = 'mydb'
measurement = 'sensor'


sensor_map = {
    '11200005': 'lab-HumidityTemp',
    '11200006': 'lab1-HumidityTemp',
}

def db_exists():
    '''returns True if the database exists'''
    dbs = client.get_list_database()
    for db in dbs:
        if db['name'] == dbname:
            return True
    return False

def wait_for_server(host, port, nretries=5):
    '''wait for the server to come online for waiting_time, nretries times.'''
    url = 'http://{}:{}'.format(host, port)
    waiting_time = 1
    for i in range(nretries):
        try:
            requests.get(url)
            return
        except requests.exceptions.ConnectionError:
            print('waiting for', url)
            time.sleep(waiting_time)
            waiting_time *= 2
            pass
    print('cannot connect to', url)
    sys.exit(1)

def connect_db(host, port, reset):
    '''connect to the database, and create it if it does not exist'''
    global client
    print('connecting to database: {}:{}'.format(host,port))
    client = InfluxDBClient(host, port, retries=5, timeout=1)
    wait_for_server(host, port)
    create = False
    if not db_exists():
        create = True
        print('creating database...')
        client.create_database(dbname)
    else:
        print('database already exists')
    client.switch_database(dbname)
    if not create and reset:
        client.delete_series(measurement=measurement)
##----------------------------- Influx connect finish
def dataformat(data):
    try:
        ID = re.findall(r",ID:(.+?),STAT",data)
        TEMP = re.findall(r",T:(.+?)\\xa1\\xe6,", data)
        HUMIDITY = re.findall(r",H:(.+?)%,", data)
        CURRENTDT = str(datetime.datetime.now())
        return {"mac_add": ID[0], "time": CURRENTDT, "temperature": TEMP[0], "humidity": HUMIDITY[0]}
    except ValueError as e:
        logging.error("Data Formate is wrong:" + str(e))

def store_reading_influx(value):
    '''insert measurements from serial sensor to the db.
    value is formatted value from sensor received here

    sensor_map: master dictionary to store name of different sensors we storing data
    '''
    sensorName = sensor_map.get(value.mac_add,'Other')
    data = [{
        'measurement':measurement,
        #'time':datetime.datetime.now(),
        'time':value.time,        
        'tags': {
            'sensorName' : sensorName
            },
            'fields' : {
                'temperature' : value.temperature,
                'humidity':value.humidity
                },
        }]
    client.write_points(data)
    if debug: pprint.pprint(data)
    time.sleep(1)
##------------------------------ Sensor write done
'''
def measure(nmeas):
    #insert dummy measurements to the db.
    #nmeas = 0 means : insert measurements forever.

    i = 0
    if nmeas==0:
        nmeas = sys.maxsize
    for i in range(nmeas):
        x = i/10.
        y = math.sin(x)
        data = [{
            'measurement':measurement,
            'time':datetime.datetime.now(),
            'tags': {
                'x' : x
                },
                'fields' : {
                    'y' : y
                    },
            }]
        client.write_points(data)
        pprint.pprint(data)
        time.sleep(1)
'''
def get_entries():
    '''returns all entries in the database.'''
    results = client.query('select * from {}'.format(measurement))
    # we decide not to use the x tag
    return list(results[(measurement, None)])

   
if __name__ == '__main__':
    import sys
    debug=False
   
    from optparse import OptionParser
    parser = OptionParser('%prog [OPTIONS] <host> <port>')
    parser.add_option(
        '-r', '--reset', dest='reset',
        help='reset database',
        default=False,
        action='store_true'
        )
    parser.add_option(
        '-n', '--nmeasurements', dest='nmeasurements',
        type='int',
        help='reset database',
        default=0
        )
   
    options, args = parser.parse_args()
    if len(args)!=2:
        parser.print_usage()
        print('please specify two arguments')
        sys.exit(1)
    host, port = args
    #host, port = '172.14.207.32','8086'
    #connect_db(host, port, False)
    connect_db(host, port, options.reset)
    ##----------------------------------- InfluxDB connected
    try:
        ser = serial.Serial("/dev/ttyUSB0", baudrate=9600, timeout=3.0)
        #ser = serial.Serial("COM4", baudrate=9600, timeout=3.0)
    except Exception as e:
        logging.error("Port is not Working:" + str(e))
    while True:
        try:
            rcv = ser.read(300)
            if (str(rcv)[1:] != '\'\''):
                data = str(rcv)[1:]
                #data = 'GW_ID:2,TYPE:T&H,ID:286851425,STAT:00000000,T:25.9\xa1\xe6,H:62.3%,ST:5M,V:3.55v,SN:72,RSSI:-48dBm,S:22.5769,E:113.9712,Time:0-0-0 0:0:0,T_RSSI:-80dBm\r\n'
                try:
                    value = dataformat(data)
                    try:
                        store_reading_influx(value)
                    except:
                        logging.error("Failed in writing: " + str(value))

                    if debug: print(value)
                except Exception as e:
                    logging.error("record value:" + str(data))
                    logging.error("record error:" + str(e))
        except:
            try:
                time.sleep(10)
                ser = serial.Serial("/dev/ttyUSB0", baudrate=9600, timeout=3.0)
            except:
                logging.error("Waiting for gateway.")
                break

    def signal_handler(sig, frame):
        print()
        print('stopping')
        pprint.pprint(get_entries())
        sys.exit(0)
    #signal.signal(signal.SIGINT, signal_handler)

    #measure(options.nmeasurements)
       
    #pprint.pprint(get_entries())