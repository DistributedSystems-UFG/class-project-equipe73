#from __future__ import print_function

import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *

import time
# from pynput import keyboard

def options(access):
    if access == 1 :
        print("1 -- Inserir Usuario\n")
    if access <= 2 :
        print("2 -- Alterar Leds\n")
    if access <= 3 :
        print("3 -- Verificar Temperatura\n")
    if access <= 3 :
        print("4 -- Verificar Iluminação\n")

def CreateUser(key):
    print("insira login, senha e acesso\n")
    log = input()
    pas = input()
    acc = int(input())
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.CreateUser(iot_service_pb2.UserRequest(login=log,password=pas,access=acc,key=key))
    print(response.status)

def LedBlink(key):
    print("insira estado e nome do led\n")
    state = input()
    name = input()
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.BlinkLed(iot_service_pb2.LedRequest(state=int(state),ledname=name,key = key))

    if response.status !='OK':
        print(response.status)
    elif response.ledstate[name] == 1:
        print("Led state is on")
    else:
        print("Led state is off")

def TempShow(key):
    while True:
        time.sleep(0.5)
        with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
            stub = iot_service_pb2_grpc.IoTServiceStub(channel)
            response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(key = key))
        if response.status !='OK':
            print(response.status)
            break
        print("Temperature received: " + response.temperature)
        # if keyboard.pressed("x"):
        #     break

def LightShow(key):
    while True:
        time.sleep(0.5)
        with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
            stub = iot_service_pb2_grpc.IoTServiceStub(channel)
            response = stub.SayLightLevel(iot_service_pb2.LightLevelRequest(key = key))
        if response.status !='OK':
            print(response.status)
            break
        
        print("Light level received: " + response.lightLevel + "\n")
        # if keyboard.pressed("x"):
        #     break
            

def run():
    
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        response = stub.UserLogin(iot_service_pb2.LoginRequest(login=sys.argv[1],password=sys.argv[2]))

    if response.status != 'OK':
        print(response.status)
        return
    print('Sucess logged with access ' + str(response.access))
    while True:
        options(response.access)
        at = int(input())
        match at:
            case 1:
                CreateUser(response.key)
            case 2:
                LedBlink(response.key)
            case 3:
                TempShow(response.key)
            case 4:
                LightShow(response.key)
        # if keyboard.pressed("x"):
        #     break

if __name__ == '__main__':
    logging.basicConfig()
    run()
