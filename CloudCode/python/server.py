from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
current_temperature = 'void'
temperature_access = 3
current_light_level = 'void'
light_access = 3
led_state = {'red':0, 'green':0}
led_access = {'red':2, 'green':2}
# três tipos de acesso definidos como:
# 1 -> admin pode tudo, inclusive criar usuarios
# 2 -> pode tudo menos criar usuarios
# 3 -> pode receber info, mas não editar

DB=[
 {
 'login':'admin',
 'password':'admin',
 'access':1
 },
 {
 'login':'user',
 'password':'user',
 'access':2
 }]

last_key = 1

#quando um usuario realiza o login, uma chave é atribuida a ele,
#definindo o nivel de acesso da chave, 
#todos os proximos requests são feitos usando a chave como parametro.
TA=[
    {
        'key': 1,
        'login':'admin',
        'access': 1
    }
]

#Aqui são os comando de conexão com o raspberry, mantidos os originais
# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print ('Received Temperature: ', msg.value.decode())
        current_temperature = msg.value.decode()
# Kafka consumer to run on a separate thread
def consume_light_level():
    global current_light_level
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print ('Received Light Level: ', msg.value.decode())
        current_light_level = msg.value.decode()
def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state
        
#verifica o acesso
def verify(key,needed):
    ta = [ t for t in TA if (t['key'] == key) ]
    if not ta:
        return 'Login Necessary'
    if ta[0]['access'] > needed:
        return 'Access Denied'
    return 'OK'
    


class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def CreateUser(self, request, context):
        usr = {
            'login':request.login,
            'password':request.password,
            'access':request.access
        }
        DB.append(usr)
        return iot_service_pb2.StatusReply(status='OK')
    
    def UserLogin(self, request, context):
        global last_key
        usr = [ us for us in DB if (us['login'] == request.login) ] 
        if not usr:
            return iot_service_pb2.LoginReply(status='User Doesnt Exists',key = -1,access = -1)
        usr = [ us for us in usr if (us['password'] == request.password) ] 
        if not usr :
            return iot_service_pb2.LoginReply(status='Wrong Password',key = -1,access = -1)
        my_key= last_key
        last_key += 1
        ta = {
            'key':my_key,
            'login':usr[0]['login'],
            'access':usr[0]['access']
        }
        TA.append(ta)
        return iot_service_pb2.LoginReply(status='OK',key = my_key,access = usr[0]['access'])
    
    def SayTemperature(self, request, context):
        ver = verify(request.key,temperature_access)
        if ver != 'OK':
            iot_service_pb2.TemperatureReply(status=ver,temperature=-1)
        return iot_service_pb2.TemperatureReply(status=ver,temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        ver = verify(request.key,led_access[request.ledname])
        if ver != 'OK':
            iot_service_pb2.LedReply(status=ver,ledstate=led_state)
        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        led_state[request.ledname] = request.state
        return iot_service_pb2.LedReply(status=ver,ledstate=led_state)

    def SayLightLevel(self, request, context):
        ver = verify(request.key,temperature_access)
        if ver != 'OK':
            iot_service_pb2.TemperatureReply(status=ver,temperature=-1)
        return iot_service_pb2.LightLevelReply(status='OK',lightLevel=current_light_level)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()

    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
