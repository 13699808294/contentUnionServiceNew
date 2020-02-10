import json
import time

from tornado import gen

from apps.contentUnion.meetingRoom import MeetingRoom
from setting.setting import DATABASES, QOS_LOCAL, QOS, OFFLINE
from utils.bMosquittoClient import BMosquittoClient
from utils.baseAsync import BaseAsync

from utils.MysqlClient import  mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps


class MosquittoClient(BMosquittoClient):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.meeting_room_list = []
        self.GuidToMeetingRoom = {}
        self.virtual_meeting_room_list = []
        self.GuidToVirtualMeetingRoom = {}
        self.separate_meeting_room_list = []
        self.heartTopic = '/aaiot/contentUnion/send/controlbus/system/heartbeat'
        # 获取所有的会议室
        self.ioloop.add_timeout(self.ioloop.time(), self.initAllMeetingRoom)
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.testGatewayStatus)

    #mqtt收到信息
    @gen.coroutine
    def handleOnMessage(self,mReceiveMessageObject):
        # yield logClient.tornadoDebugLog('主题:{},数据:{}'.format(mReceiveMessageObject.topic,mReceiveMessageObject.data))
        guid = mReceiveMessageObject.guid
        if guid == 0 or guid == '0':
            type_ = mReceiveMessageObject.type
            if type_ == 'websocket':
                channel = mReceiveMessageObject.channel
                data = mReceiveMessageObject.data
                try:
                    data = json.loads(data)
                except:
                    return
                if channel == 'info':
                    yield self.get_meeting_room_info(data)
                elif channel == 'control' or channel == 'query':
                    yield self.control_device(channel, data)
                elif channel == 'meeting_room_status':
                    yield self.get_meeting_room_status(data)
                elif channel == 'meeting_room_device':
                    yield self.get_device_info(data)
                elif channel == 'device_channel':
                    yield self.get_device_channel(data)
                elif channel == 'meeting_room_schedule':
                    yield self.getMeetingRoomSchedule(data)
            elif type_ == 'time':
                for meeting_room_object in self.meeting_room_list:
                    yield meeting_room_object.MeetingRoomOnMessage(mReceiveMessageObject)
        else:
            # 获取会议室对象
            meeting_room_object = self.GuidToMeetingRoom.get(guid)
            if meeting_room_object == None:
                return
            yield meeting_room_object.MeetingRoomOnMessage(mReceiveMessageObject)

    #mqtt连接成功
    @gen.coroutine
    def handle_on_connect(self):
        topic = '/aaiot/mqttService/receive/controlbus/system/heartbeat'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/0/receive/controlbus/event/websocket/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的websocket事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/receive/controlbus/system/#'  # 使用模式2订阅所有网关发送给会议室的心跳数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/button/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/channel/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/level/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/string/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/matrix/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/command/#'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))

        topic = '/aaiot/+/receive/controlbus/event/matrix/0/0'  # 使用模式2订阅所有网关发送给会议室的button数据
        self.connectObject.subscribe(topic, qos=QOS)
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS))
        #
        # 监控作用
        # topic = '/aaiot/+/send/controlbus/event/#'
        # self.mosquittoClient.subscribe(topic, qos=QOS)  # 使用模式0订阅,自己发送给网关的控制数据
        # topic = '/aaiot/+/send/controlbus/system/#'
        # self.mosquittoClient.subscribe(topic, qos=QOS)  # 使用模式0订阅,自己发送给网关的心跳数据

        # eventService的事件
        topic = '/aaiot/+/send/controlbus/event/schedule/schedule_info/0'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/schedule/remaining_time/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/schedule/schedule_time/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))
        topic = '/aaiot/+/send/controlbus/event/recognition/0/0'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的识别事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/schedule/add'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/schedule/remove'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/schedule/change'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的排程事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/update/0/0'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地同步事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

        topic = '/aaiot/+/send/controlbus/event/time/#'
        self.connectObject.subscribe(topic, qos=QOS_LOCAL)  # 使用模式0订阅,本地服务的时间事件
        yield logClient.tornadoDebugLog('订阅主题:({}),qos=({})'.format(topic, QOS_LOCAL))

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
        meeting_room_guid = msg.get('meeting_room_guid')
        meeting_room_object = self.GuidToMeetingRoom.get(meeting_room_guid)
        if meeting_room_object is None:
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
                yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)
        else:
            status_info = yield meeting_room_object.get_meeting_room_status()
            content = {
                'ret': 0,
                'type': 'meeting_room_status',
                'status_info': status_info,
                'meeting_room_guid':meeting_room_guid,
                'connection_token': connection_token
            }
            topic = '/aaiot/0/send/controlbus/event/websocket/0/meeting_room_status'
            yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)

    @gen.coroutine
    def getMeetingRoomSchedule(self,msg):
        connection_token = msg.get('connection_token')
        meeting_room_guid = msg.get('meeting_room_guid')
        meeting_room_object = self.GuidToMeetingRoom.get(meeting_room_guid)
        if meeting_room_object is not None:
            schedule_info = yield meeting_room_object.getSelfScheduleInfo()
            content = {
                'ret': 0,
                'type': 'meeting_room_schedule',
                'schedule_info': schedule_info,
                'meeting_room_guid': meeting_room_guid,
                'connection_token': connection_token
            }
            topic = '/aaiot/0/send/controlbus/event/websocket/0/meeting_room_schedule'
            yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)

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
        use_room_list = msg.get('use_room_list',[])
        if not isinstance(use_room_list,list):
            return
        for meeting_room_guid in use_room_list:
            if meeting_room_guid == None:
                continue
            #实体会议室
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
                            yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
                        elif separate_group_info['separate_mode'] == 1:
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
                            yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
            else:
                #虚拟会议室
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
                    #分房会议室
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
                                    yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
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
                                    yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

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
            yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

    @gen.coroutine
    def getMeetingRoomChannelInfo(self,meeting_room_object):
        info_ = yield meeting_room_object.getAllChannelInfo()
        meeting_room_guid = meeting_room_object.guid
        # 获取分配通道
        meeting_room_allocate_dict = self.allocate_channel_dict.get(meeting_room_guid,{})
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
            yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)

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
            'connection_token': connection_token
        }
        topic = '/aaiot/0/send/controlbus/event/websocket/0/device_channel'
        yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)

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
        yield self.myPublish(topic, json_dumps(content), QOS_LOCAL)

    #todo：间隔30秒检测网关链接状态
    def testGatewayStatus(self):
        now_time = time.time()
        for meeting_room_object in self.meeting_room_list:
            if now_time - meeting_room_object.lastHeartbeat >= 30:
                meeting_room_object.updateGatewayStatus(OFFLINE)
        self.ioloop.add_timeout(self.ioloop.time() + 1, self.testGatewayStatus)


    #todo :获取所有分配通道
    @gen.coroutine
    def getAllocateChannelGuid(self):
        self.allocate_channel_dict = {}
        for DATABASE in DATABASES:
            db = DATABASE['name']
            # 获取通道分配信息
            data = {
                'database': db,
                'fields': ['meeting_room_guid_id', 'control_channel_guid_id'],
                'eq': {
                    'is_delete': False,
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_allocate_channel', data)
            if msg['ret'] == '0':
                allocate_channel_list = msg['msg']
            else:
                yield logClient.tornadoErrorLog('数据库{}：查询错误:({})'.format(db, 'd_allocate_channel'))
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

    # todo:获取所有的会议室
    @gen.coroutine
    def initAllMeetingRoom(self):
        '''获取所有的会议室,更新全局变量meeting_room_list'''
        yield self.getAllocateChannelGuid()
        # 获取所有的会议室
        for DATABASE in DATABASES:
            db = DATABASE['name']
            # 获取公司名称
            if db == 'aura':
                db_name = 'default'
            else:
                db_name = db
            data = {
                'database': db,
                # 'database':'aura',
                'fields': ['name'],
                'eq': {
                    'database_name': db_name,
                }
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_company', data)
            if msg['ret'] == '0' and msg['lenght'] == 1:
                company_name = msg['msg'][0]['name']
            else:
                yield logClient.tornadoErrorLog('数据库:{},查询错误:({})'.format(db, 'd_company'))
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
                meeting_room_ = MeetingRoom(db, mosquitto_object=self, **meeting_room)
                self.meeting_room_list.append(meeting_room_)
                # 添加会议室guid和mqtt_name的转换对象
                self.GuidToMeetingRoom[meeting_room_.guid] = meeting_room_

            # 获取虚拟会议室
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
                # 获取虚拟会议室下的会议室
                data = {
                    'database': db,
                    'fields': ['guid', 'real_meeting_room_guid_id',
                               ],
                    'eq': {
                        'is_delete': False,
                        'virtual_meeting_room_guid_id': virtual_meeting_room_guid,
                    },
                }
                msg = yield mysqlClient.tornadoSelectAll('d_virtual_meeting_room', data)
                if msg['ret'] == '0':
                    meeting_room['real_meeting_room_guid_list'] = [x['real_meeting_room_guid_id'] for x in
                                                                   msg['msg']]
                else:
                    yield logClient.tornadoErrorLog('数据库查询错误:({})'.format('d_meeting_room'))
                    meeting_room['real_meeting_room_guid_list'] = []
                self.virtual_meeting_room_list.append(meeting_room)
                self.GuidToVirtualMeetingRoom[virtual_meeting_room_guid] = meeting_room
        # 获取分房会议室
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
                'database': company_db,
                'fields': ['separate_meeting_room_guid_id'],
                'eq': {
                    'real_meeting_room_guid_id': real_meeting_room_guid,
                    'is_delete': False
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
                        'guid': separete_meeting_room_guid,
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

    #todo :更新分配通道
    @gen.coroutine
    def updateAllocateChannel(self, channel_object):
        for meeting_room_guid, allocate_dict in self.allocate_channel_dict.items():
            if channel_object.guid in self.allocate_channel_dict[meeting_room_guid].keys():
                self.allocate_channel_dict[meeting_room_guid][channel_object.guid] = channel_object