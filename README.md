# SSU-2022_2-StarterProject

This repository contains the three main components used in the class project, namely:

1. IoT Code: Code to run on the Raspberry Pi, which will act as an IoT device with sensors and actuators. Obs.: Without the Raspberry Pi kits, you may run this part on a usual machine (either local or on the cloud), simulating the temperature sensor (e.g., a routine that generates random temperature values) and the actuator (led - a simple on-off variable).

2. Cloud Code: Code to run on the cloud servers - Kafka consumer and producer; gRPC IoT service

3. Client Code: command line gRPC clients of the IoT service

## Steps to run the demo:

### a. Start Kafka on a cloud-based server (server-1):

$ bin/zookeeper-server-start.sh config/zookeeper.properties

$ bin/kafka-server-start.sh config/server.properties

(If necessary, edit the server.properties file to change the IP address of the Kafka Broker) 


### b. On another cloud-based server (server-2):

- Install gRPC for Python - see instructions on https://grpc.io/docs/languages/python/quickstart/

- Clone the repo: 

$ git clone https://github.com/professorfabio/SSU-2022_2-StarterProject.git
- Compile the interface (protocol buffer definition):

$ cd CloudCode/python

$ python3 -m grpc_tools.protoc -I../protos --python_out=. --grpc_python_out=. ../protos/iot_service.proto

- Run virtual_device_service.py (it contains the cloud-based Consumer and Producer, and well as the gRPC service):

$ python3 virtual_device_service.py

(If necessary, edit the const.py file with the IP address of the Kafka Broker -- server-1)

### c. On the Raspberry Pi:

- Install the Kafka Python client:

$ pip3 install kafka-python

(If necessary install python3-pip first)

(Also if necessary, read this instructions to enable communication with the temperature sensore via GPIO: https://www.waveshare.com/wiki/Raspberry_Pi_Tutorial_Series:_1-Wire_DS18B20_Sensor)

(Also if necessary, edit the const.py file with the address of the gRPC server -- server-2)

- Clone the repo:

$ git clone https://github.com/professorfabio/SSU-2022_2-StarterProject.git

(If necessary, install git)

- Run device-controler.py (it contains IoT-based Producer and Consumer, which produce events from sensors and consume events for the actuators)

$ cd IoTCode

$ python3 device-controler.py

### d. On a client machine (may be on the cloud or on a local machine):

- Compile the interface:

$ cd ClientCode/python

$ python3 -m grpc_tools.protoc -I../protos --python_out=. --grpc_python_out=. ../protos/iot_service.proto

- Run the client code (for led control and thermometer access)

$ python3 thermometer_client.py

$ python3 led_client.py 1 red  --or-- $ python3 led_client.py 0 red (turn on and off, respectively. Just examples)

(If necessary, edit the const.py file with the IP address of the gRPC server -- server-2)

## Overall structure of the system

![image](https://user-images.githubusercontent.com/13460193/204534405-b17b1abb-77e1-479a-8171-807dc610ee5d.png)
