import base64
import json
import os
import paho.mqtt.client as mqtt
import sanic
from sanic import Sanic
import sys
import logging
import base64
import struct
from sanic.log import logger as log

import sqlite3 as sldb

APPNAME='SmartDelta'

tablename='DateSmartDelta2'
bazadedate='SmartDelta.db'

#MQTT_HOST = '192.168.241.21'
MQTT_HOST='127.0.0.1'
MQTT_PORT = 1883

MQTT_TOPIC_PREFIX = 'smartdelta-lora-test'

MY_GW = 'pygate-ana2809'

app = Sanic(APPNAME)


sir="f"*12
_LORA_PKG_FORMAT ="!"+sir
parametri=["appid", "devid", "gwid", "gweui", "timestamp", "snr", "rssi", "received",
           "bw", "sf", "cr", "freq", "altitudine","presiune","temperatura", "umiditate", "punct_roua",
           "Lumina_B","Lumina_R","acceleratiex","acceleratiey","acceleratiez","roll","pitch"]

async def overtake_metadata(request):
    # chestii relevante (RSSI, ID gateway...) -- nu prea multe
    # METADATE = request.json['metadata']
    UPLINKMSG= request.json['uplink_message']
    print(UPLINKMSG)
    payloadbytes = UPLINKMSG['decoded_payload']['bytes']
    print(payloadbytes)
    ab = bytearray(payloadbytes)
    altitudine, presiune, temperatura, umiditate, punct_roua, B_lux, R_lux,xacc, yacc, zacc, rolll, pitchh=struct.unpack(_LORA_PKG_FORMAT, ab)
    print("Altitudine:", altitudine)
    print("Presiune atmosferica", presiune)
    print("Temperatura:", temperatura)
    print("Umiditate:", umiditate)
    print("Punct de roua:", punct_roua)
    print("Lumina, canal B:", B_lux)
    print("Lumina, canal R:", R_lux)
    print("Acceleratie axa x:", xacc)
    print("Acceleratie axa y:", yacc)
    print("Acceleratie axa z:", zacc)
    print("Roll", rolll)
    print("Pitch", pitchh)
    receivedat = request.json['received_at']
    SETTINGS= UPLINKMSG['settings']
    DATARATE=SETTINGS['data_rate']
    LORA=DATARATE['lora']
    bw=LORA['bandwidth']
    sf=LORA['spreading_factor']
    cr=SETTINGS['coding_rate']
    freq=SETTINGS['frequency']
    TIMEONAIR=UPLINKMSG['consumed_airtime']
    print(TIMEONAIR)
    METADATE = UPLINKMSG['rx_metadata']
    l_META=len(METADATE)


    for i in range(l_META):
        GATEWAY=METADATE[i]
        GATEWAYID=GATEWAY['gateway_ids']['gateway_id']
        GATEWAYEUI = GATEWAY['gateway_ids']['eui']
        timestamp=GATEWAY['timestamp']
        rssi = GATEWAY['rssi']
        snr = GATEWAY['snr']
        print(type(snr))
        print(type(rssi))
        print(type(GATEWAYID))
        if GATEWAYID == MY_GW:
            print('Parameters of our gateway are: SNR: {}, RSSI: {}, gateway id: {}:'.format(snr, rssi, GATEWAYID))


        #de vazut cand avem mai multe GW, cum le stocam si unde


    APP_ID = request.json['end_device_ids']['application_ids']['application_id']
    DEVICE_ID = request.json['end_device_ids']['device_id']
    print(APP_ID)
    print('/////////////////')
    ls=[APP_ID, DEVICE_ID, GATEWAYID, GATEWAYEUI, timestamp, snr,rssi, receivedat, bw, sf, cr, freq, altitudine, presiune, temperatura, umiditate, punct_roua, B_lux, R_lux,xacc, yacc, zacc, rolll, pitchh]
    return ls
def createtable(tablename):
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                appid TEXT,
                devid TEXT,
                gwid TEXT,
                gweui TEXT,
                timestamp INTEGER,
                snr REAL,
                rssi REAL,
                received TEXT,
                bw REAL,
                sf INTEGER, 
                cr TEXT,
                freq REAL,
                altitudine REAL,
                presiune REAL,
                temperatura REAL,
                umiditate REAL,
                punct_roua REAL,
                Lumina_B REAL,
                Lumina_R REAL,
                acceleratiex REAL,
                acceleratiey REAL,
                acceleratiez REAL,
                roll REAL,
                pitch REAL
            );
        """.format(tablename))
    return 0

def select_all_tasks(con,tablename):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = con.cursor()
    cur.execute("SELECT * FROM {}".format(tablename))

    rows = cur.fetchall()

    for row in rows:
        print(row)
async def appendentry(ls,tablename):
    formatare="?,"*23
    formatare="("+formatare+"?) ;"
    sir_f="INSERT INTO {} (appid, devid, gwid, gweui, timestamp, snr, rssi, received, bw, sf, cr, freq, altitudine,presiune,temperatura, umiditate, punct_roua,Lumina_B,Lumina_R,acceleratiex,acceleratiey,acceleratiez,roll,pitch) VALUES "+formatare
    sql = sir_f.format(tablename)
    # data = (ls)
    data=tuple(ls)
    cursor=con.cursor()
    cursor.execute(sql,data)
    con.commit()
    print('Insertion ok')
    cursor.close()
    return 0

async def send_mqtt(parametri, ls):
    APP_ID = ls[0]
    device_id = ls[1]
    mqtt_topic = '{prefix}/{app_id}/{device_id}'.format(prefix=MQTT_TOPIC_PREFIX, app_id=APP_ID, device_id=device_id)
    d = {}
    if len(parametri) == len(ls):
        for i in range(len(parametri)):
            d[parametri[i]] = str(ls[i])
        mqtt_payload = json.dumps(d)
        print(mqtt_payload, type(mqtt_topic))
        app.mqtt_client.publish(mqtt_topic, mqtt_payload, qos=2)
        print("Payload trimis")
        print("Topic mqtt", mqtt_topic)
    else:
        print("Nr. de parametri nu coincide cu nr. de valori!")

@app.post('/')
async def test(request):
    print(request)
    # authorization_header=request.headers['authorization']
    # if authorization_header == "beiawasp29032021":
    while (True):
        ls = await overtake_metadata(request)
        await appendentry(ls, tablename)
        select_all_tasks(con, tablename)
        await send_mqtt(parametri,ls)
        return sanic.response.json({'status': 'ok'})
    # else:
    #     log.error('Incorrect Authorization Header!')
    #     return sanic.response.json({'status': 'invalid authorization header'})


if __name__ == '__main__':
    print('//////')
    con = sldb.connect(bazadedate)
    createtable(tablename)

    app.mqtt_client = mqtt.Client()
    app.mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    log.info ("Running with MQTT HOST %s and MQTT PORT %s", MQTT_HOST, MQTT_PORT)
    app.mqtt_client.loop_start()
    app.run(debug=True, access_log=False)

##e cu 3 ore in urma!