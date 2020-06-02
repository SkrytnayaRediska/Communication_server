#!/usr/bin/env python3

from multiprocessing import Process, current_process
import sys
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser
import threading, socket, json, pika, logging
import time


def _get_instance_name(instance):
    return instance.__class__.__module__ + "." + instance.__class__.__name__ + ".0x.." + hex(id(instance))[-2:]


def _get_instance_name(instance):
    return instance.__class__.__module__ + "." + instance.__class__.__name__ + ".0x.." + hex(id(instance))[-2:]


class LoggingMix():
    def __init__(self, instance):
        self.instance = instance

    def __getattr__(self, name):
        if name in ['critical', 'error', 'warning', 'info', 'debug']:
            if not hasattr(self.__class__, '__logger'):
                logging.basicConfig(filename="mylog.log",
                                    level=logging.INFO,
                                    format='%(asctime)s %(name)s %(levelname)s:%(message)s')
                self.__class__.__logger = logging.getLogger(_get_instance_name(self.instance))
                    #(self.__class__.__module__)

            return getattr(self.__class__.__logger, name)
        return super(LoggingMix, self).__getattr__(name)


class Broker():
    def __init__(self):
        self.broker_logger = LoggingMix(self)
        self.mess = ''
    def send_mess(self, message):
        self.mess = message
        broker_connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='127.0.0.1'))
        broker_channel = broker_connection.channel()
        broker_channel.queue_declare(queue="mess")
        broker_channel.basic_publish(exchange='', routing_key="mess",
                                     body=self.mess)
        self.broker_logger.info(" [x] Sent to broker: %s" % str(self.mess))
        broker_connection.close()



class ServerProc(Process):

    def __init__(self, port):
        self.port = port
        Process.__init__(self)
        self.serv_proc_logger = LoggingMix(self)
        self.connection = None
        self.max_client = 999
        self.recv_buffer = 2000
        self.host = '127.0.0.1'
        self.CONNS = []

    def run(self):
        curr_proc = current_process()
        self.serv_proc_logger.info('Starting process %s, ID %s...'
                                   % (curr_proc.name, curr_proc.pid))
        self.serv_proc_logger.info('Port = %s' % str(self.port))
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.bind((self.host, self.port))
        self.connection.listen(self.max_client)

        while True:
            self.serv_proc_logger.info('Running on %s: %s.'
                                       % (self.host, str(self.port)))
            channel, details = self.connection.accept()
            self.serv_proc_logger.info('Conect on : %s' % str(details))
            self.CONNS.append(ServerThread(channel, details))
            self.serv_proc_logger.info("CONNS: %s" % self.CONNS)
            self.CONNS[-1].start()

        connection.close()


class ServerThread(threading.Thread):
    def __init__(self, client_sock, addr):
        self.serv_thread_logger = LoggingMix(self)
        self.client_sock = client_sock
        self.addr = addr
        self.recv_buffer = 2000
        self.rabbit_key = ""
        self.rabbit_message = list()
        threading.Thread.__init__(self)

    def run(self):
        while True:
            recv_data = self.client_sock.recv(self.recv_buffer)
            if not recv_data:
                self.serv_thread_logger.info("Close connnection")
                break
            else:
                self.serv_thread_logger.info("Received: %s" % recv_data.decode("utf-8"))
                rabbit_key = ""
                if self.check_login_wialon(recv_data):
                    self.serv_thread_logger.info("Login is OK")
                    self.rabbit_key = self.get_rabbit_key_wialon(recv_data)
                rabbit_message = list()
                if self.check_data_wialon(recv_data):
                    self.serv_thread_logger.info("Data is OK")
                    self.rabbit_message = self.get_rabbit_message_wialon(recv_data)

                if (self.rabbit_key != "") and (len(self.rabbit_message) != 0):
                    self.serv_thread_logger.info("Sending wialon packet to broker...")
                    self.send_message_to_broker(self.rabbit_key, self.rabbit_message)
                    self.serv_thread_logger.info("Wialon packet is sent.")

    def check_login_wialon(self, recv_login):
        check_log_packet = (recv_login.decode('utf-8'))[0] + (recv_login.decode('utf-8'))[1]
        if check_log_packet != "#L":
            return False
        else:
            return True

    def check_data_wialon(self, recv_data):
        check_log_packet = (recv_data.decode('utf-8'))[0] + (recv_data.decode('utf-8'))[1]
        if check_log_packet != "#D":
            return False
        else:
            return True

    def get_rabbit_key_wialon(self, recv_login):
        log_split_lst = (recv_login.decode('utf-8')).split("#")
        imei_split_lst = (log_split_lst[2].split(";"))
        imei = imei_split_lst[0]
        rabbit_key = str(log_split_lst[1]) + imei
        return rabbit_key

    def get_rabbit_message_wialon(self, recv_data):
        data_pack = (recv_data.decode("utf-8"))[3:len(recv_data.decode("utf-8"))]
        data_pack_list = data_pack.split(";")
        return data_pack_list

    def send_message_to_broker(self, rabbit_key, data_pack_list):
        date = data_pack_list[0]
        time = data_pack_list[1]
        lat = data_pack_list[2]
        lon = data_pack_list[4]
        speed = data_pack_list[6]
        course = data_pack_list[7]
        height = data_pack_list[8]
        sats = data_pack_list[9]
        hdop = data_pack_list[10]
        inputs = data_pack_list[11]
        outputs = data_pack_list[12]
        adc = data_pack_list[13]
        ibutton = data_pack_list[14]
        params = (data_pack_list[15]).split(",")
        message_for_broker = json.dumps({str(rabbit_key): {'date': date, 'time': time,
                                                           'lat': lat, 'lon': lon, 'speed': speed, 'course': course,
                                                           'height': height, 'sats': sats, 'hdop': hdop,
                                                           'inputs': inputs,
                                                           'outputs': outputs, 'adc': adc, 'ibutton': ibutton,
                                                           'params': params}},
                                        separators=(',', ':'), indent=4)
        broker = Broker()
        broker.send_mess(message_for_broker)
        '''broker_connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='127.0.0.1'))
        broker_channel = broker_connection.channel()
        broker_channel.queue_declare(queue="mess")
        broker_channel.basic_publish(exchange='', routing_key="mess",
                                     body=message_for_broker)
        self.serv_thread_logger.info(" [x] Sent to broker: %s" % str(message_for_broker))
        broker_connection.close()'''


conf = configparser.RawConfigParser()
conf.read("config_ports.conf")
ports = list()
ports.append(int(conf.get("PORTS", "port1")))
ports.append(int(conf.get("PORTS", "port2")))
ports.append(int(conf.get("PORTS", "port3")))
for i in range(0, 3):
    my_proc = ServerProc(ports[i])
    my_proc.start()


