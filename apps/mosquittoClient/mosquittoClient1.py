import asyncio
import datetime
import json
import os
import time
import uuid
from json import JSONDecodeError

import paho.mqtt.client as mqtt
import uvloop
from tornado import gen
from tornado.concurrent import run_on_executor
from tornado.platform.asyncio import BaseAsyncIOLoop
from concurrent.futures import ThreadPoolExecutor
from apps.contentUnion.meetingRoom import MeetingRoom
from setting.setting import QOS, MQTT_SERVICE_HOST, DEBUG, OFFLINE, MY_SQL_SERVER_HOST, DATABASES, QOS_LOCAL, BASE_DIR
from utils.MysqlClient import  mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps


class MosquittoClient():
    executor = ThreadPoolExecutor(max_workers=10)
    def __init__(self,host=MQTT_SERVICE_HOST,port=1883,username='test001',password='test001',keepalive=60,bind_address='',ioloop=None,aioloop=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.keepalive = keepalive
        self.bind_address = bind_address

        self.connect_status = False
        self.ioloop = ioloop
        self.aioloop = aioloop
        self.meeting_room_list = []
        self.GuidToMeetingRoom = {}
        self.virtual_meeting_room_list = []
        self.GuidToVirtualMeetingRoom = {}
        self.separate_meeting_room_list = []
        # self.GuidToSeparateMeetingRoom = {}
        self.mosquittoReceiveBuffer = []
        self.handle_message_task = None
        self.publish_buffer = []
        self.publish_buffer_task = None
        self.heartTopic = '/aaiot/contentUnion/send/controlbus/system/heartbeat'
        self.heartInterval = 1

        self.mosquittoClient = mqtt.Client(client_id="service.contentUnionService.{}".format(uuid.uuid1()), clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.mosquittoClient.username_pw_set(username=self.username, password=self.password)
        if self.port == 8883 or self.port == 18883:
            self.mosquittoClient.tls_set(ca_certs=os.path.join(BASE_DIR, 'mosquitto_ssl_file_taopu/ca/ca.crt'),
                                         certfile=os.path.join(BASE_DIR, 'mosquitto_ssl_file_taopu/client/client.crt'),
                                         keyfile=os.path.join(BASE_DIR, 'mosquitto_ssl_file_taopu/client/client.key')
                                         )
            self.mosquittoClient.max_inflight_messages_set(200)
            self.mosquittoClient.max_queued_messages_set(1000)

        self.mosquittoClient.on_connect = self.on_connect
        self.mosquittoClient.on_disconnect = self.on_disconnect
        self.mosquittoClient.on_message = self.on_message
        try:
            self.mosquittoClient.connect(host=self.host,port=self.port,keepalive=self.keepalive,bind_address=self.bind_address)
        except:
            logClient.infoLog('mqtt连接失败')
            self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

        self.ioloop.add_timeout(self.ioloop.time(), self.mosquittoClientLoop)
        self.ioloop.add_timeout(self.ioloop.time() + self.heartInterval, self.sendHeart)
        # 获取所有的会议室
        self.ioloop.add_timeout(self.ioloop.time(), self.initAllMeetingRoom)
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.testGatewayStatus)

    @gen.coroutine
    def my_publish(self,topic, payload=None, qos=0, retain=False):
        if self.connect_status:
            self.mosquittoClient.publish(topic,payload,qos,retain)
        else:
            msg = {
                'topic':topic,
                'payload':payload,
                'qos':qos,
                'retain':retain,
            }
            self.publish_buffer.append(msg)

            if self.publish_buffer_task == None:
                self.publish_buffer_task = 1
                asyncio.ensure_future(self.publish_buffer_info(), loop=self.aioloop).add_done_callback(self.publishCallback)

    @gen.coroutine
    def publishCallback(self,futu):
        self.publish_buffer_task = None

    @gen.coroutine
    def publish_buffer_info(self):
        if self.connect_status:
            while True:
                try:
                    msg = self.publish_buffer.pop(0)
                except:
                    self.publish_buffer_task = None
                    return
                self.mosquittoClient.publish(**msg)
        self.publish_buffer_task = None

    #todo:获取所有的会议室
    @gen.coroutine
    def initAllMeetingRoom(self):
        '''获取所有的会议室,更新全局变量meeting_room_list'''
        yield self.getAllocateChannelGuid()
        # 获取所有的会议室
        for DATABASE in DATABASES:
            db = DATABASE['name']
            #获取公司名称
            if db == 'aura':
                db_name = 'default'
            else:
                db_name = db
            data = {
                'database':db,
                # 'database':'aura',
                'fields':['name'],
                'eq':{
                    'database_name':db_name,
                }
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_company', data)
            if msg['ret'] == '0' and msg['lenght'] == 1:
                company_name = msg['msg'][0]['name']
            else:
                yield logClient.tornadoErrorLog('数据库:{},查询错误:({})'.format(db,'d_company'))
                continue

            data = {
                'database': db,
                'fields': [
                    'guid',
                    'room_name',
                    'status_group_guid_id as schedule_status',
                ],
                'eq': {
                    'is_delete': False,
                    # 'guid':'ec5eadb8-9744-11e9-922b-5254002d0365',
                    'virtual_guid': '',
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)

            if msg['ret'] == '0':
                meeting_rooms = msg['msg']
            else:
                yield logClient.tornadoErrorLog('数据库查询错误:({})'.format('d_meeting_room'))
                continue
            for meeting_room in meeting_rooms:
                meeting_room['company_name'] = company_name
                # 把每一个会议室转换为会议室对象
                meeting_room_ = MeetingRoom(db,self,**meeting_room)
                self.meeting_room_list.append(meeting_room_)
                # 添加会议室guid和mqtt_name的转换对象
                self.GuidToMeetingRoom[meeting_room_.guid] = meeting_room_

            #获取虚拟会议室
            data = {
                'database': db,
                'fields': [
                    'guid',
                    'room_name',
                    'status_group_guid_id as schedule_status',
                ],
                'eq': {
                    'is_delete': False,
                    'virtual_guid': 'virtual',
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)
            if msg['ret'] == '0':
                meeting_rooms = msg['msg']
            else:
                yield logClient.tornadoErrorLog('数据库查询错误:({})'.format('d_meeting_room'))
                continue
            for meeting_room in meeting_rooms:
                meeting_room['company_db'] = db
                meeting_room['company_name'] = company_name
                virtual_meeting_room_guid = meeting_room.get('guid')
                #获取虚拟会议室下的会议室
                data = {
                    'database': db,
                    'fields': ['guid','real_meeting_room_guid_id',
                    ],
                    'eq': {
                        'is_delete': False,
                        'virtual_meeting_room_guid_id': virtual_meeting_room_guid,
                    },
                }
                msg = yield mysqlClient.tornadoSelectAll('d_virtual_meeting_room', data)
                if msg['ret'] == '0':
                    meeting_room['real_meeting_room_guid_list'] = [x['real_meeting_room_guid_id'] for x in msg['msg']]
                else:
                    yield logClient.tornadoErrorLog('数据库查询错误:({})'.format('d_meeting_room'))
                    meeting_room['real_meeting_room_guid_list'] = []
                self.virtual_meeting_room_list.append(meeting_room)
                self.GuidToVirtualMeetingRoom[virtual_meeting_room_guid] = meeting_room
        #获取分房会议室
        '''
        分房会议室结构：
        self.separate_meeting_room_list = [
            {
                'real_meeting_room_guid':
                'separate_mode':0/1,
                'meeting_room_info':[
                    {
                        'guid':'',
                        'room_name':'',
                        'schedule_status':'',
                        'company':'',
                        'company_name':'',
                    }
                ]
            }
        ]
        '''
        for meeting_room_object in self.meeting_room_list:
            real_meeting_room_guid = meeting_room_object.guid
            company_db = meeting_room_object.company_db
            data = {
                'database':company_db,
                'fields':['separate_meeting_room_guid_id'],
                'eq':{
                    'real_meeting_room_guid_id':real_meeting_room_guid,
                    'is_delete':False
                }
            }
            msg = yield mysqlClient.tornadoSelectAll('d_separate_meeting_room', data)
            if msg['ret'] == '0':
                separete_meeting_room_guid_list = [x['separate_meeting_room_guid_id'] for x in msg['msg']]
            else:
                yield logClient.tornadoErrorLog('数据库查询错误:({})'.format('d_meeting_room'))
                continue
            if separete_meeting_room_guid_list == []:
                continue
            separete_group_info = {}
            separete_group_info['real_meeting_room_guid'] = real_meeting_room_guid
            separete_group_info['separate_mode'] = 0
            separete_group_info['meeting_room_info'] = []
            for separete_meeting_room_guid in separete_meeting_room_guid_list:
                data = {
                    'database': company_db,
                    'fields': [
                        'guid',
                        'room_name',
                        'status_group_guid_id as schedule_status',
                    ],
                    'eq': {
                        'is_delete': False,
                        'virtual_guid': 'separate',
                        'guid':separete_meeting_room_guid,
                    },
                }
                msg = yield mysqlClient.tornadoSelectOnly('d_meeting_room', data)
                if msg['ret'] == '0' and msg['lenght'] == 1:
                    separete_info = msg['msg'][0]
                    separete_info['company_db'] = company_db
                else:
                    continue
                separete_group_info['meeting_room_info'].append(separete_info)
            self.separate_meeting_room_list.append(separete_group_info)
        else:
            pass

    @gen.coroutine
    def getAllocateChannelGuid(self):
        self.allocate_channel_dict = {}
        for DATABASE in DATABASES:
            db = DATABASE['name']
            # 获取通道分配信息
            data = {
                'database': db,
                'fields': ['meeting_room_guid_id','control_channel_guid_id'],
                'eq': {
                    'is_delete': False,
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_allocate_channel', data)
            if msg['ret'] == '0':
                allocate_channel_list = msg['msg']
            else:
                yield logClient.tornadoErrorLog('数据库{}：查询错误:({})'.format(db,'d_allocate_channel'))
                continue
            for allocate_channel_info in allocate_channel_list:
                meeting_room_guid = allocate_channel_info.get('meeting_room_guid_id')
                allocate_channel_guid = allocate_channel_info.get('control_channel_guid_id')
                if meeting_room_guid not in self.allocate_channel_dict.keys():
                    self.allocate_channel_dict[meeting_room_guid] = {}
                    self.allocate_channel_dict[meeting_room_guid][allocate_channel_guid] = None
                else:
                    self.allocate_channel_dict[meeting_room_guid][allocate_channel_guid] = None
        else:
            pass

    @gen.coroutine
    def updateAllocateChannel(self,channel_object):
        for meeting_room_guid,allocate_dict in self.allocate_channel_dict.items():
            if channel_object.guid in self.allocate_channel_dict[meeting_room_guid].keys():
                self.allocate_channel_dict[meeting_room_guid][channel_object.guid] = channel_object

    #todo:收到信息回调
    def on_message(self,client, userdata, message):
        #1 获取主题,数据
        topic = message.topic
        topic_list = self.TopicToList(topic)
        # logClient.tornadoDebugLog(topic+' '+message.payload.decode())
        # if len(topic_list) < 6:
        #     return
        data = {
            'topic': topic,
            'data': message.payload.decode(),
            'topic_list':topic_list
        }
        self.mosquittoReceiveBuffer.append(data)
        if self.handle_message_task == None:
            self.handle_message_task = 1
            asyncio.ensure_future(self.handle_message(), loop=self.aioloop).add_done_callback(self.handleMessageCallback)

    @gen.coroutine
    def handleMessageCallback(self,futu):
        self.handle_message_task = None
        # yield logClient.tornadoDebugLog('mosquitto 处理 on_message 回调')

    @gen.coroutine
    def handle_message(self):
        try:
            while self.mosquittoReceiveBuffer:
                try:
                    msg = self.mosquittoReceiveBuffer.pop(0)
                except:
                    return
                topic_list = msg.get('topic_list')
                if topic_list[1] == 0 or topic_list[1] == '0':
                    if topic_list[5] == 'websocket':
                        data = msg.get('data')
                        try:
                            data = json.loads(data)
                        except :
                            return
                        if topic_list[7] == 'info':
                            yield self.get_meeting_room_info(data)
                            # pass
                        elif topic_list[7] == 'control' or topic_list[7] == 'query':
                            yield self.control_device(topic_list[7],data)
                            # pass
                        elif topic_list[7] == 'meeting_room_status':
                            yield self.get_meeting_room_status(data)
                        elif topic_list[7] == 'meeting_room_device':
                            yield self.get_device_info(data)
                        elif topic_list[7] == 'device_channel':
                            yield self.get_device_channel(data)
                    elif topic_list[5] == 'time':
                        for meeting_room_object in self.meeting_room_list:
                            yield meeting_room_object.MeetingRoomOnMessage(msg)
                else:
                    # 获取会议室对象
                    meeting_room_object = self.GuidToMeetingRoom.get(topic_list[1])
                    if meeting_room_object == None:
                        continue
                    yield meeting_room_object.MeetingRoomOnMessage(msg)
        except:
            self.handle_message_task = None

    @gen.coroutine
    def get_meeting_room_status(self,msg):
        '''
        msg = {
            'ret': 0,
            'type': request_type,
            'company_db':company_db,
            'connection_token': connection_token,
        }
        '''
        connection_token = msg.get('connection_token')
        company_db = msg.get('company_db')
        status_info_list = []
        for meeting_room_object in self.meeting_room_list:
            if meeting_room_object.company_db == company_db:
                info = yield meeting_room_object.get_meeting_room_status()
                status_info_list.append(info)
        else:
            content = {
                'ret': 0,
                'type': 'meeting_room_status',
                'meeting_room_status_info_list': status_info_list,
                'errmsg': '',
                'connection_token': connection_token
            }
            topic = '/aaiot/0/send/controlbus/event/websocket/0/meeting_room_status'
            yield self.my_publish(topic, json_dumps(content), QOS_LOCAL)

    @gen.coroutine
    def get_meeting_room_info(self, msg):
        '''
        msg = {
                'ret':0,
                'type': request_type,
                'use_room_list':use_room_list,
                'connection_token':connection_token,
            }
        '''
        yield logClient.tornadoDebugLog('获取会议室info')
        connection_token = msg.get('connection_token')
        control_info = msg.get('control_info')
        room_list_info = []
        # 获取房间信息
        use_room_list = msg.get('use_room_list')
        if use_room_list == None:
            return
        for meeting_room_guid in use_room_list:
            if meeting_room_guid == None:
                continue
            # 判断是否是 == 实体会议室
            meeting_room_object = self.GuidToMeetingRoom.get(meeting_room_guid)
            if meeting_room_object:
                channel_info = yield self.getMeetingRoomChannelInfo(meeting_room_object)
                room_list_info.append(channel_info)
                for separate_group_info in self.separate_meeting_room_list:
                    real_meeting_room_guid = separate_group_info.get('real_meeting_room_guid')
                    if real_meeting_room_guid == meeting_room_guid:
                        if control_info:
                            separate_group_info['separate_mode'] = 0
                            msg = {
                                'ret': 0,
                                'type': 'separate_change',
                                'mode': separate_group_info.get('separate_mode'),
                                'meeting_room_info': {
                                    'real_meeting_room_guid': real_meeting_room_guid,
                                    'separate_meeting_room_guid_list': [x['guid'] for x in separate_group_info.get('meeting_room_info')]
                                }
                            }
                            topic = '/aaiot/0/send/controlbus/event/websocket/0/separete_change'
                            yield self.my_publish(topic, json_dumps(msg), QOS_LOCAL)
                        elif separate_group_info['separate_mode'] == 1:
                            # control_info = 1
                            msg = {
                                'ret': 0,
                                'type': 'separate_change',
                                'mode': separate_group_info.get('separate_mode'),
                                'meeting_room_info': {
                                    'real_meeting_room_guid': real_meeting_room_guid,
                                    'separate_meeting_room_guid_list': [x['guid'] for x in
                                                                        separate_group_info.get('meeting_room_info')]
                                }
                            }
                            topic = '/aaiot/0/send/controlbus/event/websocket/0/separete_change'
                            yield self.my_publish(topic, json_dumps(msg), QOS_LOCAL)
            else:
                # 判断是否是 == 虚拟会议室
                virtual_meeting_room_object = self.GuidToVirtualMeetingRoom.get(meeting_room_guid)
                if virtual_meeting_room_object != None:
                    real_meeting_room_guid_list = virtual_meeting_room_object.get('real_meeting_room_guid_list')
                    for real_meeting_room_guid in real_meeting_room_guid_list:
                        if real_meeting_room_guid in use_room_list:
                            continue
                        real_meeting_room_object = self.GuidToMeetingRoom.get(real_meeting_room_guid)
                        if real_meeting_room_object == None:
                            continue
                        channel_info = yield self.getMeetingRoomChannelInfo(real_meeting_room_object)
                        room_list_info.append(channel_info)
                else:
                    # 判断是否是 == 分房会议室
                    for separate_group_info in self.separate_meeting_room_list:
                        real_meeting_room_guid = separate_group_info.get('real_meeting_room_guid')
                        for meeting_room_info in separate_group_info.get('meeting_room_info'):
                            guid = meeting_room_info.get('guid')
                            if meeting_room_guid == guid:
                                if control_info:
                                    separate_group_info['separate_mode'] = 1
                                    # 进入分房模式
                                    msg = {
                                        'ret': 0,
                                        'type': 'separate_change',
                                        'mode': separate_group_info.get('separate_mode'),
                                        'meeting_room_info': {
                                            'real_meeting_room_guid': real_meeting_room_guid,
                                            'separate_meeting_room_guid_list': [x['guid'] for x in separate_group_info.get('meeting_room_info')]
                                        }
                                    }
                                    topic = '/aaiot/0/send/controlbus/event/websocket/0/separete_change'
                                    yield self.my_publish(topic, json_dumps(msg), QOS_LOCAL)
                                elif separate_group_info['separate_mode'] == 0:
                                    # 进入分房模式
                                    # control_info = 1
                                    msg = {
                                        'ret': 0,
                                        'type': 'separate_change',
                                        'mode': separate_group_info.get('separate_mode'),
                                        'meeting_room_info': {
                                            'real_meeting_room_guid': real_meeting_room_guid,
                                            'separate_meeting_room_guid_list': [x['guid'] for x in separate_group_info.get('meeting_room_info')]
                                        }
                                    }
                                    topic = '/aaiot/0/send/controlbus/event/websocket/0/separete_change'
                                    yield self.my_publish(topic, json_dumps(msg), QOS_LOCAL)

                                real_meeting_room_object = self.GuidToMeetingRoom.get(real_meeting_room_guid)
                                if real_meeting_room_object == None:
                                    break
                                channel_info = yield self.getMeetingRoomChannelInfo(real_meeting_room_object)
                                #更改會議室名稱,和guid
                                channel_info['room_name'] = meeting_room_info.get('room_name')
                                channel_info['room_guid'] = meeting_room_info.get('guid')
                                room_list_info.append(channel_info)
                                break
                        else:
                            continue
                        break
        if not control_info:
            msg = {
                'ret': 0,
                'type': 'info',
                'room_info_list': room_list_info,
                'connection_token': connection_token
            }
            topic = '/aaiot/0/send/controlbus/event/websocket/0/info'
            yield self.my_publish(topic, json_dumps(msg), QOS_LOCAL)

    @gen.coroutine
    def getMeetingRoomChannelInfo(self,meeting_room_object):
        info_ = yield meeting_room_object.getAllChannelInfo()
        meeting_room_guid = meeting_room_object.guid
        # 获取分配通道
        meeting_room_allocate_dict = self.allocate_channel_dict.get(meeting_room_guid)
        if meeting_room_allocate_dict:
            for channel_guid, channel_obejct in meeting_room_allocate_dict.items():
                if channel_obejct == None:
                    continue
                channel_type = channel_obejct.channel_type_guid_id
                channel_info = yield channel_obejct.getSelfInfo()
                try:
                    info_[channel_type].append(channel_info)
                except:
                    pass
        return info_

    @gen.coroutine
    def get_device_info(self,msg):
        connection_token = msg.get('connection_token')
        company_db = msg.get('company_db')
        meeting_room_guid = msg.get('meeting_room_guid')
        device_info_list = []
        for meeting_room_object in self.meeting_room_list:
            if meeting_room_guid:
                result = meeting_room_guid == meeting_room_object.guid
            elif company_db:
                result = meeting_room_object.company_db == company_db
            else:
                continue
            if result:
                try:
                    info = yield meeting_room_object.get_meeting_room_device()
                    device_info_list.append(info)
                except Exception as e:
                    print(str(e))
        else:
            content = {
                'ret': 0,
                'type': 'meeting_room_device',
                'meeting_room_device_info_list': device_info_list,
                'errmsg': '',
                'connection_token': connection_token
            }
            topic = '/aaiot/0/send/controlbus/event/websocket/0/meeting_room_device'
            yield self.my_publish(topic, json_dumps(content), QOS_LOCAL)

    @gen.coroutine
    def get_device_channel(self,msg):
        connection_token = msg.get('connection_token')
        meeting_room_guid = msg.get('meeting_room_guid')
        device_guid = msg.get('device_guid')
        meeting_room_object = self.GuidToMeetingRoom.get(meeting_room_guid)
        if meeting_room_object == None:
            return
        device_channel = yield meeting_room_object.getDeviceChannelInfo(device_guid)

        content = {
            'ret': 0,
            'type': 'device_channel',
            'meeting_room_guid':meeting_room_guid,
            'device_guid':device_guid,
            'device_channel': device_channel,
            'errmsg': '',
            'connection_token': connection_token
        }
        topic = '/aaiot/0/send/controlbus/event/websocket/0/device_channel'
        yield self.my_publish(topic, json_dumps(content), QOS_LOCAL)

    #todo:控制设备
    @gen.coroutine
    def control_device(self,control_type,msg):
        '''
        msg = {
            'ret':0,
            'type':'control',
            'meeting_room_guid':'会议室guid',
            'channel_guid':'通道guid',
            'user_id':'用户id',
            'event_type':'事件类型',
            'status':'当前状态',
            'control_type':'控制类型'
            ‘input_port’:'连接的输入源guid
            'connection_token':'连接token‘
        }
        '''
        # msg = {
        #     'ret': 0,
        #     'type': 'control',
        #     'meeting_room_guid': '会议室guid',
        #     'channel_guid': '通道guid',
        #     'event_type': 'click',
        #     'status': '当前状态',
        #     'control_type': '控制类型',
        #     'input_port':'连接的输入源guid',
        #     'connection_token':'连接token',
        # }
        connection_token = msg.get('connection_token')
        meeting_room_guid = msg.get('meeting_room_guid')
        channel_guid = msg.get('channel_guid')

        meeting_room_object = self.GuidToMeetingRoom.get(meeting_room_guid)
        if meeting_room_object == None:
            yield logClient.tornadoWarningLog('控制未知会议室设备')
            return
        back_msg = yield meeting_room_object.userControl(msg)
        ret = back_msg.get('ret')
        if ret == 0:
            content = {
                'ret': 0,
                'type': control_type,
                'guid': channel_guid,
                'control_event': back_msg.get('event_type'),
                'status': back_msg.get('status'),
                'connection_token': connection_token
            }
        else:
            content = {
                'ret': 1,
                'type': control_type,
                'guid': channel_guid,
                'connection_token': connection_token
            }
        topic = '/aaiot/0/send/controlbus/event/websocket/0/control'
        yield self.my_publish(topic, json_dumps(content), QOS_LOCAL)

    #todo：间隔30秒检测网关链接状态
    def testGatewayStatus(self):
        now_time = time.time()
        for meeting_room_object in self.meeting_room_list:
            if now_time - meeting_room_object.lastHeartbeat >= 30:
                meeting_room_object.updateGatewayStatus(OFFLINE)
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.testGatewayStatus)

    #todo:保持mosquitto连接
    def keepConnect(self):
        if self.connect_status == False:
            try:
                self.mosquittoClient.reconnect()
            except:
                logClient.debugLog('mqtt重连失败,尝试继续重连')
                self.ioloop.add_timeout(self.ioloop.time() + 5, self.keepConnect)

    #todo：循环维持
    @gen.coroutine
    def mosquittoClientLoop(self):
        self.mosquittoClient.loop(timeout=0.001)
        if self.connect_status == True:
            self.ioloop.add_timeout(self.ioloop.time(),self.mosquittoClientLoop)
        else:
            self.ioloop.add_timeout(self.ioloop.time()+0.1, self.mosquittoClientLoop)

    #todo:异步喜欢维持
    @gen.coroutine
    def mosquittoClientAsyncLoop(self):
        yield self.mosquittoClientLoop()
        if self.connect_status == True:
            self.ioloop.add_timeout(self.ioloop.time(),self.mosquittoClientAsyncLoop)
        else:
            self.ioloop.add_timeout(self.ioloop.time()+0.1, self.mosquittoClientAsyncLoop)

    #todo：发送心跳
    def sendHeart(self):
        yield self.my_publish(self.heartTopic, json.dumps(time.time()), qos=QOS_LOCAL)
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
            logClient.infoLog('mqtt连接成功')
            topic = '/aaiot/mqttService/receive/controlbus/system/heartbeat'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic,QOS_LOCAL))

            topic = '/aaiot/0/receive/controlbus/event/websocket/#'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的websocket事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/receive/controlbus/system/#'  # 使用模式2订阅所有网关发送给会议室的心跳数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/button/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/channel/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/level/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/string/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/matrix/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/command/#'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

            topic = '/aaiot/+/receive/controlbus/event/matrix/0/0'  # 使用模式2订阅所有网关发送给会议室的button数据
            self.mosquittoClient.subscribe(topic, qos=QOS)
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS))
            #
            # 监控作用
            # topic = '/aaiot/+/send/controlbus/event/#'
            # self.mosquittoClient.subscribe(topic, qos=QOS)  # 使用模式0订阅,自己发送给网关的控制数据
            # topic = '/aaiot/+/send/controlbus/system/#'
            # self.mosquittoClient.subscribe(topic, qos=QOS)  # 使用模式0订阅,自己发送给网关的心跳数据

            # eventService的事件
            topic = '/aaiot/+/send/controlbus/event/schedule/schedule_info/0'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/schedule/remaining_time/#'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/schedule/schedule_time/#'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))
            topic = '/aaiot/+/send/controlbus/event/recognition/0/0'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的识别事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/schedule/add'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/schedule/remove'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/schedule/change'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/update/0/0'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地同步事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

            topic = '/aaiot/+/send/controlbus/event/time/#'
            self.mosquittoClient.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的时间事件
            logClient.debugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

    def on_disconnect(self,client, userdata, rc):
        self.connect_status = False
        logClient.infoLog('mqtt断开连接,启动重连')
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.keepConnect)

    def on_publish(self,client, userdata, mid):
        print('发布回调',mid)

    def on_subscribe(self,client, userdata, mid, granted_qos):
        print('订阅成功')

    def on_unsubscribe(self,client, userdata, mid):
        print('订阅失败')

    def on_log(self,client, userdata, level, buf):
        print('输出日志')

    def TopicToList(self,topic):
        topic_list = topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        return topic_list
