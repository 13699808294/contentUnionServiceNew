import asyncio
import datetime
import json
import os
import time
import paho.mqtt.client as mqtt
from tornado import gen
from tornado.ioloop import PeriodicCallback

from setting.setting import QOS_LOCAL, BASE_DIR
from utils.baseAsync import BaseAsync
from utils.mReceiveMessage import MReceiveMessage
from utils.mPublishMessage import MPublishMessage


class BMosquittoClient(BaseAsync):
    def __init__(self,host='127.0.0.1',port=1883,username='test001',password='test001',keepalive=60,bind_address='',**option):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.bind_address = bind_address
        self.connect_status = False

        self.heartTopic = ''
        self.heartInterval = 15
        self.handle_message_task = None
        self.ReceiveBuffer = []
        self.publish_buffer_task = None
        self.publishBuffer = []
        self.connectObject = mqtt.Client(client_id="", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.connectObject.username_pw_set(username=self.username, password=self.password)
        if self.port >= 8883:
            ssl_file = option.get('ssl_file')
            if ssl_file == None:
                raise ValueError('ssl_file is not find')
            self.connectObject.tls_set(ca_certs=os.path.join(BASE_DIR, '{}/ca/ca.crt'),
                                         certfile=os.path.join(BASE_DIR, '{}/client/client.crt'),
                                         keyfile=os.path.join(BASE_DIR, '{}/client/client.key')
                                         )
            self.connectObject.max_inflight_messages_set(200)
            self.connectObject.max_queued_messages_set(1000)

        self.connectObject.on_connect = self.on_connect
        self.connectObject.on_disconnect = self.on_disconnect
        self.connectObject.on_message = self.on_message
        try:
            self.connectObject.connect(host=self.host,port=self.port,keepalive=self.keepalive,bind_address=self.bind_address)
        except:
            print('[{}] [SystemOut]: '.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + 'mqtt断开连接,启动重连')
            self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

        self.periodicCallbackTask = PeriodicCallback(self.connectObjectLoop,0.0001)
        self.switchLoop()
        #
        self.ioloop.add_timeout(self.ioloop.time() + self.heartInterval, self.sendHeart)

    def keepConnect(self):
        if self.connect_status == False:
            try:
                self.connectObject.reconnect()
            except:
                print('[{}] [SystemOut]: '.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + 'mqtt重连失败,继续尝试重连')
                self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)


    def switchLoop(self):
        if self.connect_status == True:
            self.periodicCallbackTask.start()
        else:
            self.periodicCallbackTask.stop()
            self.ioloop.add_timeout(self.ioloop.time() + 0.01, self.connectObjectLoop)

    @gen.coroutine
    def connectObjectLoop(self):
        if self.connect_status == True:
            self.connectObject.loop(timeout=0.003)
        else:
            self.connectObject.loop(timeout=0.01)
            self.ioloop.add_timeout(self.ioloop.time()+0.01,self.connectObjectLoop)

    #todo：发送心跳
    def sendHeart(self):
        if self.connect_status == True:
            self.connectObject.publish(self.heartTopic, json.dumps(time.time()), qos=QOS_LOCAL)
            self.ioloop.add_timeout(self.ioloop.time() + self.heartInterval, self.sendHeart)

    #todo：设置心态主题
    def updateHeartTopic(self,heartTopic):
        self.heartTopic = heartTopic

    #todo：设置心跳间隔
    def updateHeartInterval(self,second):
        self.heartInterval = second

    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            self.connect_status = True
            self.switchLoop()
            print('[{}] [SystemOut]: '.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + 'mqtt连接成功')
            if self.publishBuffer:
                if self.publish_buffer_task == None:
                    self.publish_buffer_task = 1
                    asyncio.ensure_future(self.publishBufferInfo(), loop=self.aioloop).add_done_callback(self.publishCallback)
            self.ioloop.add_timeout(self.ioloop.time(),self.handle_on_connect)

    def on_disconnect(self,client, userdata, rc):
        self.connect_status = False
        self.switchLoop()
        print('[{}] [SystemOut]: '.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + 'mqtt断开连接,启动重连')
        self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

    def on_message(self,client, userdata, message):
        mReceiveMessageObject = MReceiveMessage(client, userdata, message)
        self.ReceiveBuffer.append(mReceiveMessageObject)
        if self.handle_message_task == None:
            self.handle_message_task = 1
            asyncio.ensure_future(self.handle_on_message(), loop=self.aioloop).add_done_callback(self.handleMessageCallback)

    def on_publish(self,client, userdata, mid):
        pass

    def on_subscribe(self,client, userdata, mid, granted_qos):
        pass

    def on_unsubscribe(self,client, userdata, mid):
        pass

    def on_log(self,client, userdata, level, buf):
        pass

    @gen.coroutine
    def handleMessageCallback(self, futu):
        self.handle_message_task = None

    @gen.coroutine
    def handle_on_message(self):
        while self.ReceiveBuffer:
            try:
                mReceiveMessageObject = self.ReceiveBuffer.pop(0)
                yield self.handleOnMessage(mReceiveMessageObject)
            except Exception as e:
                self.handle_message_task = None
                break
        else:
            self.handle_message_task = None
    @gen.coroutine
    def handleOnMessage(self,mReceiveMessageObject):
        pass

    @gen.coroutine
    def handle_on_connect(self):
        pass

    @gen.coroutine
    def myPublish(self, topic, payload=None, qos=0, retain=False):
        publishMessage = MPublishMessage(topic=topic,payload=payload,qos=qos,retain=retain)
        if self.connect_status:
            self.connectObject.publish(publishMessage.topic, publishMessage.payload, publishMessage.qos, publishMessage.retain)
        else:
            self.publishBuffer.append(publishMessage)

    @gen.coroutine
    def myPublishAioloop(self,topic, payload=None, qos=0, retain=False):
        publishMessage = MPublishMessage(topic=topic, payload=payload, qos=qos, retain=retain)
        if self.connect_status:
            self.connectObject.publish(publishMessage.topic, publishMessage.payload, publishMessage.qos,publishMessage.retain)
        else:
            self.publishBuffer.append(publishMessage)

    @gen.coroutine
    def publishCallback(self, futu):
        self.publish_buffer_task = None

    @gen.coroutine
    def publishBufferInfo(self):
        if self.connect_status:
            while True:
                try:
                    publishMessage = self.publishBuffer.pop(0)
                except:
                    self.publish_buffer_task = None
                    return
                self.connectObject.publish(publishMessage.topic, publishMessage.payload, publishMessage.qos, publishMessage.retain)
        self.publish_buffer_task = None