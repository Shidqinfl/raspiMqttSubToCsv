from socket import gaierror
import paho.mqtt.client as mqtt
import ssl
from datetime import datetime as dt
import csv

csv_path = '/home/pi/aws/aws_data_log.csv'

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("[INFO] Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("AWS32/data")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    datetime_now = dt.now()
    now = datetime_now.strftime("%m/%d/%Y, %H:%M:%S")
    print("Timestamp: "+now+", "+msg.topic+" "+str(msg.payload))
    data = "Timestamp: "+now+", "+msg.topic+" "+str(msg.payload)
    store_data(data)

def store_data(data):
    f = open('aws_data_log.csv','a')
    #with open(csv_path, 'wb') as f:
    #    for line in data:
    #        f.write(data)
    #        f.write('\n')
    #        f.close()
    f.write(data)
    f.write('\n')
    f.close()
    return ("[INFO] Save data successful!")

def main():
    client = mqtt.Client()
    #client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    #client.tls_set("/etc/ssl/certs/ca-certificates.crt")
    #client.tls_insecure_set(True)

    try:
        client.username_pw_set(username="iot", password="iot")
    except Exception as e:
        print(e)

    print("[INFO] Connecting...")
    try:
        client.connect("monitoringku.com", 1883, 60)
    except Exception as e:
        print(e)
    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()
    # Non blocking : client.loop_start()  N.B. need a while True: statement

if __name__ == "__main__":
    main()

