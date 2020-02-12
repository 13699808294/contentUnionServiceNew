import asyncio
import datetime
import json
import time
import uuid

from concurrent.futures import ThreadPoolExecutor
from json import JSONDecodeError


from tornado import gen

from apps.contentUnion.room_status import RoomStatus
from apps.contentUnion.rule import Rule
from apps.contentUnion.scene import Scene
from utils.baseAsync import BaseAsync
from utils.mReceiveMessage import MReceiveMessage
from .controlChannel import ControlChannel
from .device import Device
from setting.setting import MY_SQL_SERVER_HOST, QOS, ONLINE, TYPEBACK, QOS_LOCAL, DEBUG, USE_MACRO
from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps
# from utils.responseStatus import RET



class MeetingRoom(BaseAsync):
    executor = ThreadPoolExecutor(max_workers=100)
    def __init__(self,company_db,mosquitto_object,guid,room_name,schedule_status,company_name,):
        super().__init__()
        '''会议室信息'''
        self.guid = guid
        self.name = room_name
        self.company_name = company_name
        self.company_db = company_db
        self.mosquittoObject = mosquitto_object
        self.myPublish = self.mosquittoObject.myPublish

        #获取会议室部分主题
        self.getMeetingRoomTopic()
        #监听主题指向控制通道对象
        self.monitorTopicToObject = {}
        #获取会议室设备
        self.device_dict = {}
        #控制通道
        self.channel_dict = {}          #通道guid为键
        #聚集通道
        self.macro_channel_info_list = []
        #记录通道上传时的状态
        self.ChannelUpdataStatus = {}
        #订阅消息缓存
        self.subscribeInfoBuffer = []
        #网关心跳时间戳
        self.lastHeartbeat = time.time()

        #设备端口状态分配列表
        self.deviceStatusList = []

        #会议室状态
        self.meetingRoomStatusDict = {}

        if schedule_status == 0:
            self.schedule_status = '空闲中'
        elif schedule_status == 1:
            self.schedule_status = '准备中'
        elif schedule_status == 2:
            self.schedule_status = '进行中'
        else:
            self.schedule_status = '空闲中'
        self.schedule_change_time = 0
        self.schedule_remaining_time = 0
        self.schedule_info = {}
        self.next_schedule = {}
        self.other_schedule = []


        self.gatewayStatus = None
        self.updating = True
        self.deviceSyncing = False
        self.handle_message_task = None
        #
        self.rule_list = []
        #回调
        self.scheduleCallbackList = []
        self.timeEventCallbackList = []
        self.recognitionCallbackList = []
        #
        self.sceneDict = {}

        #更新设备
        self.ioloop.add_timeout(self.ioloop.time(), self.meetingRoomInit)

    # todo：获取会议室 部分主题
    def getMeetingRoomTopic(self):
        '''获取会议室部分主题'''
        # 接受时,接收完整的主题
        self.receiveTopic = '/aaiot/{}/receive/controlbus'.format(self.guid)
        # 发送控制指令时,头/aaiot/省略
        self.sendTopic = '/aaiot/{}/send/controlbus'.format(self.guid)

    # todo:会议室监听入口
    @gen.coroutine
    def MeetingRoomOnMessage(self, mReceiveObject):
        '''
        接收到订阅信息处理
        data = {
            'topic': message.topic,
            'msg': message.payload.decode(),
            'topic_list':topic_list
        }
        '''
        # 缓存接收信息
        self.subscribeInfoBuffer.append(mReceiveObject)
        if self.handle_message_task == None:
            self.handle_message_task = 1
            asyncio.ensure_future(self.handle_message(), loop=self.aioloop).add_done_callback(self.selfHandleMessageCallback)

    def selfHandleMessageCallback(self,futu):
        self.handle_message_task = None

    @gen.coroutine
    def handle_message(self):
        '''
        data = {
            'topic': message.topic,
            'msg': message.payload.decode(),
            'topic_list': topic_list
        }
        '''
        #更新或同步中不处理数据
        try:
            if self.updating or self.deviceSyncing:
                yield gen.sleep(1)
                return
            while self.subscribeInfoBuffer:
                try:
                    mReceiveObject = self.subscribeInfoBuffer.pop(0)
                except:
                    # yield gen.sleep(1)
                    return
                # topic_list = mReceiveObject.topicList
                direction = mReceiveObject.direction       #send or receive
                #
                if direction == 'receive':
                    yield self.handleGatewayData(mReceiveObject)
                elif direction == 'send':
                    yield self.handleLocalData(mReceiveObject)
        except:
            self.handle_message_task = None

    #todo:网关数据处理
    @gen.coroutine
    def handleGatewayData(self,mReceiveObject):
        '''
        msg = {
            'topic': message.topic,
            'msg': message.payload.decode(),
            'topic_list': topic_list
        }
        '''
        topic = mReceiveObject.topic
        # topic_list = mReceiveObject.topicList
        data = mReceiveObject.data

        event = mReceiveObject.event           # event system
        event_type = mReceiveObject.type      # button channel level string command
        if event == 'event':
            port = mReceiveObject.port #
            try:
                channel = mReceiveObject.channel  #
            except:
                return
            if event_type == 'button' or event_type == 'channel' or event_type == 'string' or event_type == 'level' or event_type == 'matrix':        # 更新控制通道状态
                yield self.updateChannelStatus(topic, data)
            elif event_type == 'command':
                if port == 'welink' and channel == 'device':        # 与welink的设备同步
                    if not data:
                        yield self.asyncDeviceToWelink()
                    else:
                        self.deviceSyncing = True
                        yield self.handleDeviceSync(0, 1, data)
                        self.deviceSyncing = False
                elif channel == 'device':                           # 同步网关设备
                    # yield logClient.tornadoDebugLog(msg.get('topic')+'  '+ str(msg.get('data')))
                    if self.deviceSyncing:
                        return
                    self.deviceSyncing = True
                    yield self.handleDeviceSync(0, 0, data)
                    self.deviceSyncing = False
                elif channel == 'macro':                            # 同步巨集
                    yield self.handleMacroSync(0, data)
        elif event == 'system' and event_type == 'heartbeat':
            self.lastHeartbeat = time.time()
            yield self.updateGatewayStatus(ONLINE)                  # 更新网关状态
            #回应网关一个心跳
            topic = '{}/system/heartbeat'.format(self.sendTopic)
            yield self.myPublish(topic, str(self.lastHeartbeat), qos=QOS)

    #todo:本地数据处理
    @gen.coroutine
    def handleLocalData(self,mReceiveObject):
        '''
        msg = {
            'topic': message.topic,
            'msg': message.payload.decode(),
            'topic_list': topic_list
        }
        '''
        # topic_list = mReceiveObject.topicList
        data = mReceiveObject.data
        # yield logClient.tornadoDebugLog(msg.get('topic') + '  ' + str(msg.get('data')))
        try:
            event = mReceiveObject.event  # event system
            event_type = mReceiveObject.type  # button channel level string command
            port = mReceiveObject.port  #
            channel = mReceiveObject.channel  #
        except:
            return
        if event == 'event':
            if event_type == 'time':
                now_time = "{}:{}:00".format(port, channel)
                for function_info in self.timeEventCallbackList:
                    function = function_info.get('function')
                    kwargs = function_info.get('kwargs')
                    yield function(now_time, **kwargs)
                if port == '0' and channel == '0':
                    for device_guid,device_object in self.device_dict.items():
                        yield device_object.dayUpdateSelfOnSwitch()
            elif event_type == 'schedule':
                yield self.handleScheduleMessage(port,channel, data)
            elif event_type == 'recognition':
                yield self.handleRecognitionMessage(data)
            elif event_type == 'update':
                yield self.localUpdate(data)
            elif event_type == 'macro':
                yield self.HandleMacroUpdate(mReceiveObject)


    #******************************************************************************************************************#
    # todo:更新会议室网关状态
    @gen.coroutine
    def updateGatewayStatus(self, status):
        '''更新会议室网关状态'''
        if status != self.gatewayStatus:
            self.gatewayStatus = status
            yield logClient.tornadoDebugLog('{}...会议室：{}.网关状态：{}'.format(self.company_db, self.name, status))
            #暂时定义名称为‘网关’的设备
            for device_guid,device_object in self.device_dict.items():
                if device_object.name == '网关':
                    device_object.updateSelfOnlineStatus(None,status)
                    break
            for device_guid,device_object in self.device_dict.items():
                if device_object.name == '网关':
                    if status == 'on':
                        device_object.updateSelfSwitchOnStatus(None,status,no_channel=1)
                    elif status == 'off':
                        device_object.updateSelfSwitchOffStatus(None, status, no_channel=1)
                    break

    #todo:更新设备在线状态
    @gen.coroutine
    def updateDeviceOnlineStatus(self,device_object):
        msg = {
            "type": "online_info",
            "meeting_room_guid": self.guid,  # 会议室guid
            "getway_status": self.gatewayStatus,  # 网关状态
            "port": device_object.port,  # 端口号
            "status": device_object.online_status,  # 端口状态
            'device_name': device_object.name,
            'device_guid': device_object.guid,
            "ret": 0,
        }
        # yield logClient.tornadoInfoLog('会议室:({}),同步设备:{},在线状态：{}'.format(self.name,device_object.name,device_object.online_status))
        topic = '{}/event/websocket/0/online_info'.format(self.sendTopic)
        yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

    #todo:同步设备开关状态
    @gen.coroutine
    def updateDeviceSwitchStatus(self,device_object):
        msg = {
            "type": "device_switch",
            "meeting_room_guid": self.guid,  # 会议室guid
            "status": device_object.switch_status,  # 端口状态
            'device_name': device_object.name,
            'device_guid': device_object.guid,
            "ret": 0,
        }
        yield logClient.tornadoDebugLog('同步设备开关状态：{}'.format(device_object.name))
        topic = '{}/event/websocket/0/device_switch'.format(self.sendTopic)
        yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

    # todo:更新控制通道状态
    @gen.coroutine
    def updateChannelStatus(self,receiveTop,data):
        '''
        receiveTop:     接收主题
        data：           接收数据
        '''
        # 尝试从字典中查找控制命令对象
        # yield logClient.tornadoDebugLog('会议室:({}),处理通道同步事件,主题:({})'.format(self.name,receiveTop))
        channel_object = self.monitorTopicToObject.get(receiveTop)
        if channel_object == None:
            return
        #更新状态
        yield channel_object.updataSelfStatus(data)
        #获取更新状态
        channel_guid = channel_object.guid
        # if channel_object.used:
        # 通道同步
        msg = {
            "type": "channel_feedback",
            "meeting_room_guid": self.guid,
            "guid": channel_guid,
            "status": channel_object.getSelfStatus(),
            "event_type": channel_object.feedback,
            'name': channel_object.name,
            "time": time.time(),  # 时间戳
            # "msg":'self sync',
            "ret": 0,
        }
        topic = '/aaiot/{}/send/controlbus/event/websocket/0/channel_feedback'.format(self.guid)
        yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

        #分配通道同步信息
        yield self.allocateSendSync(channel_guid)

    #todo: 分配通道发布同步信息
    @gen.coroutine
    def allocateSendSync(self,channel_guid):
        for meeting_room_guid,channel_dict in self.mosquittoObject.allocate_channel_dict.items():
            channel_object = channel_dict.get(channel_guid)
            if channel_object:
                msg = {
                    "type": "channel_feedback",
                    "meeting_room_guid": meeting_room_guid,
                    "guid": channel_guid,
                    "status": channel_object.getSelfStatus(),
                    "event_type": channel_object.feedback,
                    'name': channel_object.name,
                    "time": time.time(),
                    "msg": 'other sync',
                    "ret": 0,
                }
                topic = '/aaiot/{}/send/controlbus/event/websocket/0/channel_feedback'.format(self.guid)
                yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)

    # todo：排程信息更新
    @gen.coroutine
    def handleScheduleMessage(self, port,channel, data):
        # yield logClient.tornadoDebugLog('会议室:({}),处理排程事件,{}'.format(self.name,port))
        event_type = port
        #增加,删除,修改排程事件,间隔及时
        if event_type == 'add' or event_type == 'remove' or event_type == 'change':
            data = json.loads(data)
            # 按照人员分组
            member_list = data['member'].split(',')
            while '' in member_list:
                member_list.remove('')
            # 个人排除变更
            for member in member_list:
                data = {
                    'database': self.company_db,
                    'fields': ['id'],
                    'eq': {
                        'is_delete': False,
                        'username': member
                    },
                }
                msg = yield mysqlClient.tornadoSelectOne('user', data)
                if msg['ret'] != '0':
                    yield logClient.tornadoErrorLog('数据库查询错误,表:{},member'.format('user',member))
                    continue
                user_id = msg['msg']['id']
                msg = {
                    "type": "user_schedule_event",
                    "meeting_room_guid": self.guid,  # 会议室guid
                    "user_name": member,  # 用户名
                    "user_id": user_id,  # 用户id
                    "status": event_type,  # 变更事件	change
                    "ret": 0,
                }
                topic = '{}/event/websocket/0/user_schedule_event'.format(self.sendTopic)
                yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
                yield logClient.tornadoDebugLog('会议室：{}:排程信息事件'.format(self.name))
            else:
                return
        # 更新当前排除剩余时间
        elif event_type == 'remaining_time':
            remaining_time = channel
            try:
                remaining_time = int(remaining_time)
                self.schedule_remaining_time = remaining_time
            except:
                return
            schedule_guid = data.get('schedule_guid')
            # 会议室进行排程时间变更
            msg = {
                "type": "schedule_time_change",
                "meeting_room_guid": self.guid,     # 会议室guid
                'schedule_guid':schedule_guid,
                "remaining_time": remaining_time,   # 变更事件	add/remove/update
                "ret": 0,
            }
            topic = '{}/event/websocket/0/schedule_time_change'.format(self.sendTopic)
            yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
            yield logClient.tornadoDebugLog('会议室：({}):排程时间事件,剩余时间:({})'.format(self.name,remaining_time))
            return
        #更新排程信息,保护当前会议,下次会议.间隔1分钟
        elif event_type == 'schedule_info':
            data = json.loads(data)
            schedule_info = data.get('now_schedule')
            next_schedule = data.get('next_schedule')
            other_schedule = data.get('other_schedule')
            schedule_status = data.get('schedule_status')
            try:
                schedule_status = int(schedule_status)
            except:
                schedule_status = 0

            if schedule_status == 1:
                if self.schedule_status != '准备中':
                    #测试功能
                    # if self.schedule_status == '空闲中':
                    #     self.schedule_status = '准备中'
                    #     yield self.localScene()
                    self.schedule_change_time = time.time()
                    self.schedule_status = '准备中'
                    yield self.updateSelfSchedule(schedule_status)
            elif schedule_status == 2:
                if self.schedule_status != '进行中':
                    # if self.schedule_status == '空闲中':
                    #     self.schedule_status = '进行中'
                    #     yield self.localScene()
                    self.schedule_change_time = time.time()
                    self.schedule_status = '进行中'
                    yield self.updateSelfSchedule(schedule_status)
            else:
                if self.schedule_status != '空闲中':
                    # 测试功能
                    self.schedule_status = '空闲中'
                    self.schedule_change_time = time.time()
                    # self.ioloop.add_timeout(self.ioloop.time(), self.leaveScene())
                    yield self.updateSelfSchedule(schedule_status)

            if schedule_info != self.schedule_info or next_schedule != self.next_schedule or other_schedule != self.other_schedule:
                self.schedule_info = schedule_info
                self.next_schedule = next_schedule
                self.other_schedule = other_schedule
                # 会议室进行排程时间变更
                msg = {
                    "type": "schedule_info",
                    "meeting_room_guid": self.guid,         # 会议室guid
                    'schedule_status':self.schedule_status,
                    "now_schedule": self.schedule_info,
                    'next_schedule':self.next_schedule,
                    'other_schedule':self.other_schedule,
                    "ret": 0,
                }
                topic = '{}/event/websocket/0/schedule_info'.format(self.sendTopic)
                yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
                yield logClient.tornadoDebugLog('会议室：({})的排除信息更新'.format(self.name))
        #排程时间
        elif event_type == 'schedule_time':
            data = json.loads(data)
            for function_info in self.scheduleCallbackList:
                function = function_info.get('function')
                kwargs = function_info.get('kwargs')
                yield function(self.schedule_status,data,**kwargs)

    @gen.coroutine
    def getSelfScheduleInfo(self):
        info = {
            "type": "schedule_info",
            "meeting_room_guid": self.guid,  # 会议室guid
            'schedule_status': self.schedule_status,
            "now_schedule": self.schedule_info,
            'next_schedule': self.next_schedule,
            'other_schedule': self.other_schedule,
        }
        return info

    #todo:识别信息
    @gen.coroutine
    def handleRecognitionMessage(self,data):
        data = json.loads(data)
        type = data.get('type') #识别源
        user_id = data.get('user_id')
        user_type = data.get('user_type')
        user_permission = data.get('user_permission')
        device_sn = data.get('device_sn')

        for function_info in self.recognitionCallbackList:
            function = function_info.get('function')
            kwargs = function_info.get('kwargs')
            yield function(type,user_id,user_type,user_permission,device_sn,**kwargs)

    #todo:本地同步
    @gen.coroutine
    def localUpdate(self,data):
        if data == 'device':
            yield self.meetingRoomInit()
        elif data == 'rule':
            pass

    @gen.coroutine
    def updateSelfSchedule(self,schedule_status):
        # 会议室排除变更
        msg = {
            "type": "schedule_event",
            "meeting_room_guid": self.guid,     # 会议室guid
            "status": schedule_status,          # 变更事件	0 空闲中 1 准备中  2 进行中
            "ret": 0,
        }
        topic = '{}/event/websocket/0/schedule_event'.format(self.sendTopic)
        yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        yield logClient.tornadoInfoLog('会议室：({})排程状态更新为({})'.format(self.name, self.schedule_status))
    # ------------------------------------------------------------------------------------------------------------------#

    @gen.coroutine
    def HandleMacroUpdate(self,mReceiveObject):
        type_ = mReceiveObject.port
        data = mReceiveObject.data
        try:
            data = json.loads(data)
            macro_channel_guid = data.get('macro_channel_guid')
        except:
            return
        if type_ == 'add_change':
            #新增或修改
            yield self.updateOneMacroInfo(macro_channel_guid)
        elif type_ == 'remove':
            #删除
            yield self.deleteOneMacroInfo(macro_channel_guid)
    # todo：处理网关同步巨集     服务器 > 网关
    @gen.coroutine
    def handleMacroSync(self, port, data):
        macro_list = yield self.getSelfMacroInfo()
        topic = '{}/event/command/0/macro'.format(self.sendTopic)
        yield self.myPublish(topic, json_dumps(macro_list), qos=QOS)

    #todo：网关请求获取所有聚集,后台提供
    @gen.coroutine
    def getSelfMacroInfo(self):
        macro_list = []
        for macro_channel_info in self.macro_channel_info_list:
            macro_channel_guid = macro_channel_info.get('info')
            macro_channel_object = self.channel_dict.get(macro_channel_guid)
            if macro_channel_object == None:
                continue
            macro_info = {}
            macro_info['channel_guid'] = macro_channel_object.guid
            macro_info["port"] = macro_channel_object.port
            macro_info["channel"] =  macro_channel_object.channel
            macro_info["string_name"] = macro_channel_object.string_name
            macro_info["name"] =  macro_channel_object.name
            macro_info["type"] = macro_channel_object.type
            macro_info["feedback"] = macro_channel_object.feedback
            macro_info['jobs'] = {
                'on': [],
                'off': []
            }
            macro_jobs = macro_channel_info.get('jobs')
            for k,macro_channel_list in macro_jobs.items():
                if k not in ['on','off']:
                    continue
                for channel in macro_channel_list:
                    channel_info = {}
                    channel_guid = channel['channel_guid']
                    channel_object = self.channel_dict.get(channel_guid)
                    if channel_object == None:
                        yield logClient.tornadoWarningLog('会议室:({}),聚集的子通道({})不存在'.format(self.name,channel_guid))
                        continue
                    channel_info["port"] = channel_object.port
                    channel_info["channel"] = channel_object.channel
                    channel_info["event"] = channel_object.type
                    channel_info["string"] = channel_object.string_name

                    channel_info["name"] = channel['name']
                    channel_info["value"] = channel['value']
                    macro_info['jobs'][k].append(channel_info)
                    try:
                        delay = channel['after_wait']
                    except:
                        delay = None
                    if delay:
                        channel_info = {}
                        channel_info["port"] = 0
                        channel_info["channel"] = 0
                        channel_info["event"] = 'wait'
                        channel_info["string"] = ''

                        channel_info["name"] = ''
                        channel_info["value"] = delay / 1000
                        macro_info['jobs'][k].append(channel_info)
            macro_info['feedbackCondition'] = yield self.getMacroBackMessage(macro_channel_info['info'])
            macro_list.append(macro_info)

        if DEBUG:
            if macro_list:
                s = json.dumps(macro_list, sort_keys=True, indent=4, ensure_ascii=False)
                with open('./macro_file/会议室:({})macro_info.json'.format(self.name), 'w') as file:
                    file.write(s)
        return macro_list

    #todo：获取网关请求的巨集反馈信息
    @gen.coroutine
    def getMacroBackMessage(self,macro_channel_guid):
        macro_back = ''
        data = {
            'database':self.company_db,
            'fields': ['guid','macro_channel_guid_id','macro_message'],
            'eq': {
                'is_delete': False,
                'macro_channel_guid_id': macro_channel_guid
            },
        }
        msg = yield mysqlClient.tornadoSelectOne('d_macro_back', data)
        if msg['ret'] == '0':
            macro_back_info = msg['msg']
        else:
            return macro_back
        if not macro_back_info:
            return macro_back
        macro_back = macro_back_info['macro_message']
        return macro_back

    #------------------------------------------------------------------------------------------------------------------#
    #todo:检测是否正在更新
    @gen.coroutine
    def checkSelfUpdataStatus(self):
        while self.updating or self.deviceSyncing:
            yield gen.sleep(1)
            yield logClient.tornadoDebugLog('等待更新完成')
        else:
            yield logClient.tornadoDebugLog('更新已完成')
            return

    # todo:获取会议室所有通道
    @gen.coroutine
    def getAllChannelInfo(self):
        # #等待更新完成
        yield self.checkSelfUpdataStatus()

        # #获取所有通道分类
        # data = {
        #     'database': self.company_db,
        #     'fields': ['guid','type_name'],
        #     'eq': {
        #         'is_delete': False,
        #     },
        # }
        # msg = yield mysqlClient.tornadoSelectAll('d_channel_type', data)
        # if msg['ret'] == '0':
        #     channel_type_list = msg['msg']
        # else:
        #     yield logClient.tornadoErrorLog('数据库查询错误:表{}'.format('d_channel_type'))
        #     return {}
        #
        # channel_info = {}
        # channel_info['room_name'] = self.name
        # channel_info['room_guid'] = self.guid
        # channel_info['channel_info'] = []
        #
        # for channel_type in channel_type_list:
        #     info = {}
        #     info['type_name'] = channel_type['type_name']
        #     info['guid'] = channel_type['guid']
        #     info['channel_list'] = []
        #
        #     for channel_guid,channel_object in self.channel_dict.items():
        #         if channel_object.channel_type_guid_id == channel_type['guid'] and channel_object.used:    #普通通道
        #             one_channel_info = yield channel_object.getSelfInfo()
        #             info['channel_list'].append(one_channel_info)
        #     channel_info['channel_info'].append(info)
        # return channel_info
        # 获取所有通道分类
        data = {
            'database': self.company_db,
            'fields': ['guid', 'type_name'],
            'eq': {
                'is_delete': False,
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_channel_type', data)
        if msg['ret'] == '0':
            channel_type_list = msg['msg']
        else:
            yield logClient.tornadoErrorLog('数据库查询错误:表{}'.format('d_channel_type'))
            return {}
        channel_info = {}
        channel_info['room_name'] = self.name
        channel_info['room_guid'] = self.guid
        for channel_type in channel_type_list:
            channel_info[channel_type['guid']] = []
            for channel_guid, channel_object in self.channel_dict.items():
                if channel_object.channel_type_guid_id == channel_type['guid'] and channel_object.used:  # 普通通道
                    info = yield channel_object.getSelfInfo()
                    channel_info[channel_type['guid']].append(info)
        return channel_info

    #todo:控制通道
    @gen.coroutine
    def userControl(self,msg):
        '''
        msg = {
            'ret':0,
            'type':'control',
            'meeting_room_guid':'会议室guid',
            'channel_guid':'通道guid',
            'user_id':'用户id',
            'event_type':'事件类型',
            'status':'当前状态',
            'event_type':'控制类型'
            'input_port':'连接的输入源guid
            'connection_token':'连接token‘
        }
        '''
        channel_guid = msg.get('channel_guid')
        control_type = msg.get('control_type')
        event_type = msg.get('event_type')
        user_id = msg.get('user_id')
        status = msg.get('status')

        channelObject = self.channel_dict.get(channel_guid)
        if channelObject == None:
            allocate_channel_dict = self.mosquittoObject.allocate_channel_dict.get(self.guid)
            if allocate_channel_dict:
                channelObject = allocate_channel_dict.get(channel_guid)
                if not channelObject:
                    content = {'ret': 1}
                    return content
            else:
                content = {'ret': 1}
                return content
        content = channelObject.userControl(msg)
        '''
        控制成功:
        content = {
            'ret': 0,
            'guid': self.guid,
            'status': back_status,
            'event_type':event_type,
            'command_flag':command_flag
        }
        控制失败:
        content = {
            'ret':1.
        }
        查询成功：
        content = {
            'ret': 0,
            'guid': self.guid,
            'status':now_status,
        }
        '''
        command_flag = content.get('command_flag')
        if command_flag:
            if control_type == 1 or control_type == 2:
                data = content['event_type']
                if channelObject.type == 'macro':
                    yield self.executeMacroChannel(channelObject.guid,data)
                else:
                    topic = channelObject.sendTopic
                    yield self.myPublish(topic, data, qos=QOS)
            #矩阵控制
            elif control_type == 4:
                #out_channel_guid   输出源guid
                #input_port     输出源guid
                input_channel_guid = content.get('status')  #输入源guid
                input_channel_object = self.channel_dict.get(input_channel_guid)

                status = msg.get('status')
                if input_channel_object == None:
                    data = {}
                else:
                    data = {
                                'channel_guid': input_channel_object.guid,
                                'port': input_channel_object.port,
                                'channel': input_channel_object.channel,
                            }
                topic = channelObject.sendTopic
                yield self.myPublish(topic, json_dumps(data), qos=QOS)
            yield logClient.tornadoInfoLog('用户:({}),通过事件:({}),控制通道:({}),控制前状态为:({}),假反馈状态为:({})'.format(user_id, event_type,channelObject.name, status,content.get('status')))
        return content

    #todo:获取会议室状态值
    @gen.coroutine
    def getMeetingRoomStatusValue(self):
        meeting_room_status_info_list = []
        for meeting_room_status_guid,meeting_room_object in self.meetingRoomStatusDict.items():
            one_status_info = {
                'guid':meeting_room_status_guid,
                'name':meeting_room_object.name,
                'status':meeting_room_object.getStringValue()
            }
            meeting_room_status_info_list.append(one_status_info)
        return meeting_room_status_info_list

    #todo: 获取会议室状态
    @gen.coroutine
    def get_meeting_room_status(self):
        '''
        status_info = {
            'name':
            ‘温度’
            ‘空气质量’
            ‘排程状态’
            ‘当前会议室时间’
            ‘剩余时间’
            ‘下次会议时间’
        }
        '''
        #尝试获取温度,空气质量
        try:
            meeting_room_status_info_list = yield self.getMeetingRoomStatusValue()
            status_info = {
                'guid':self.guid,
                'name':self.name,
                'meeting_room_status_info_list':meeting_room_status_info_list,
                'schedule_status':self.schedule_status,
                'schedule_info':self.schedule_info,
                'schedule_remaining_time':self.schedule_remaining_time,
                'next_schedule':self.next_schedule
            }
            return status_info
        except Exception as e:
            print(e)

    #todo:获取会议室设备状态
    @gen.coroutine
    def get_meeting_room_device(self):
        device_info_list = []
        for device_guid,device_object in self.device_dict.items():
            device_info = {}
            device_info['device_guid'] = device_object.guid
            device_info['device_name'] = device_object.name
            device_info['switch_status'] = device_object.switch_status
            device_info['online_status'] = device_object.online_status
            device_info_list.append(device_info)
            pass
        info = {
            'meeting_room_name':self.name,
            'guid':self.guid,
            'device_list':device_info_list
        }
        return info

    # ------------------------------------------------------------------------------------------------------------------#
    # todo：处理welink同步设备     服务器 > welink
    @gen.coroutine
    def asyncDeviceToWelink(self):
        topic = '{}/event/command/welink/device'.format(self.sendTopic)
        device_info = self.getSelfAllDeviceInfo()
        yield self.myPublish(topic, json_dumps(device_info), qos=QOS_LOCAL)

        if DEBUG:
            if device_info:
                s = json.dumps(device_info, sort_keys=True, indent=4, ensure_ascii=False)
                with open('./device_file/会议室:({})device_info.json'.format(self.name), 'w') as file:
                    file.write(s)

    # todo：处理网关同步设备     网关 > 服务器
    @gen.coroutine
    def handleDeviceSync(self,port,device_from,data):
        '''网关同步设备到后台'''
        #1:获取 网关同步的设备列表
        try:
            msg = json.loads(data)
        except JSONDecodeError:
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,网关同步的设备信息不是json格式格式'.format(self.name))
            return
        if not isinstance(msg, dict):
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,网关同步的设备信息不是字典格式'.format(self.name))
            return

        device_list = msg.get('devices_list',[])
        if DEBUG:
            if device_list:
                s = json.dumps(device_list, sort_keys=True, indent=4, ensure_ascii=False)
                with open('./update_device_file/会议室:({})device_update_info.json'.format(self.name), 'w') as file:
                    file.write(s)

        macro_channel_list = msg.get('marco_list',[])
        if DEBUG:
            if macro_channel_list:
                s = json.dumps(macro_channel_list, sort_keys=True, indent=4, ensure_ascii=False)
                with open('./macro_back/会议室:({})macro_back.json'.format(self.name), 'w') as file:
                    file.write(s)
        '''
        matrix_channel_list = [
            {

                'channel_guid':输出源通道guid,
                'port':输出源通道port,
                'channel':输出源通道channel,
                'connection_channel':
                    {
                        'channel_guid':输入源通道guid,
                        'port':输入源通道port,
                        'channel':输入源通道channel,
                    },
            },
            {},
            {},...
        ]
        '''
        if not isinstance(device_list, list):
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,网关同步的设备列表不是列表格式'.format(self.name))
            return
        if not isinstance(macro_channel_list, list):
            #
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,网关同步的macro通道不是列表格式'.format(self.name))
            return
        if not device_list:
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,网关同步的设备列表为空'.format(self.name))
            return
        #2:清空记录通道状态表
        self.ChannelUpdataStatus = {}
        #3:获取数据库设备列表
        data = {
            'database': self.company_db,
            'fields': ['guid'],
            'eq': {
                'is_delete': False,
                'device_from':device_from,
                'meeting_room_guid_id': self.guid
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('会议室：({}),同步设备,获取数据库已有设备失败'.format(self.name))
            return
        my_device_list = [x['guid'] for x in msg['msg']]
        #4: 新增,更新设备
        for device_info in device_list:
            if not isinstance(device_info, dict):
                yield logClient.tornadoWarningLog('会议室：({}),同步设备,一条同步设备信息不是字典格式'.format(self.name))
                continue
            # 获取参数
            meeting_room_guid = device_info.get('room_id')

            channel_list = device_info.get('function_list')
            guid = device_info.get('device_guid')
            name = device_info.get('device_name')
            port = device_info.get('port')
            if meeting_room_guid == self.guid:
                #设备预更新结果为失败
                result = False
                #1:遍历会议室已有设备
                if guid in my_device_list:                            #在会议室中查询guid相同的设备
                    result = yield self.updateDevice(guid,name,port)    #1.1:更新设备
                else:       #2:在会议室中没有查询guid相同的设备
                    # 判断是否有同一guid被删除过
                    data = {
                        'database': self.company_db,
                        'fields': ['guid'],
                        'eq': {
                            'guid': guid,
                            'is_delete':True,
                        },
                    }
                    msg = yield mysqlClient.tornadoSelectOnly('d_device', data)
                    if msg['ret'] != '0':
                        yield logClient.tornadoErrorLog('会议室：({}),同步设备,获取被删除的设备失败'.format(self.name))
                    else:
                        if msg['lenght'] == 0:  result = yield self.addDevice(device_from,guid,name,port)
                        else:                   result = yield self.updateDevice(guid, name, port)
                # 成功更新设备后,同步该设备的通道
                if result:
                    yield self.channelSync(guid,name, port, channel_list)
                else:
                    yield logClient.tornadoErrorLog('会议室：({}),同步设备,更新/新增会议室设备失败'.format(self.name))
            else:
                yield logClient.tornadoWarningLog('会议室：({}),同步设备,发现同步设备会议室guid或mqtt_id不正确'.format(self.name))
                # 设备不在本会议室的通道
                # yield self.channelSync('','其他会议室设备',port, channel_list)

        #5:比较删除设备
        # for device_guid in my_device_list:
        #     for device_info in device_list:
        #         if device_guid == device_info['device_guid']:
        #             break
        #     else:
        #         yield self.deleteDevice(device_guid)    #找到要删除的设备

        #6: 重新初始化 设备和通道
        yield self.updateSelfControlChannel1()
        yield self.updateSelfDevice1()
        yield self.updateChannelToDevice()

        # 获取同步巨集状态
        for macro_channel_info in macro_channel_list:
            self.ChannelUpdataStatus[macro_channel_info['channel_guid']] = macro_channel_info['channel_value']
            pass
        #8:同步所有通道的状态
        for channel_guid,status in self.ChannelUpdataStatus.items():
            if not channel_guid:
                continue
            channel_object = self.channel_dict.get(channel_guid)
            if channel_object == None:
                continue
            topic = channel_object.receiveTopic
            if not topic:
                continue
            # data = {
            #     'topic': topic,
            #     'data': status,
            #     'topic_list': self.TopicToList(topic)
            # }
            # self.subscribeInfoBuffer.append(data)
            mReceiveMessageObject = MReceiveMessage(self.mosquittoObject.connectObject, '', topic=topic,data=status)
            self.subscribeInfoBuffer.append(mReceiveMessageObject)
        else:
            if self.handle_message_task == None:
                self.handle_message_task = self.ioloop.add_timeout(self.ioloop.time(), self.handle_message)
        #7:同步数据到welink
        yield self.asyncDeviceToWelink()
        yield self.updateMacroChannel()
        yield self.initSelfScene()
        yield self.initAllMeetingRoomStatus()
        yield self.initAllRule()
        pass

    #todo：同步通道

    @gen.coroutine
    def channelSync(self,device_guid,device_name,device_port,channel_list):
        #新增,更新通道
        if not isinstance(channel_list, list):
            return
        if not channel_list:
            return
        #获取设备所有数据库通道
        data = {
            'database': self.company_db,
            'fields': ['guid'],
            'eq': {
                'is_delete': False,
                'device_guid_id': device_guid,
                'meeting_room_guid_id':self.guid,
            },
            'neq':{
                'type':5,
            }
        }
        msg = yield mysqlClient.tornadoSelectAll('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询错误:{}'.format('d_control_channel'))
            return
        my_channel_list = [x['guid'] for x in msg['msg']]
        for channel_info in channel_list:
            if not isinstance(channel_info, dict):
                continue
            #更新结果预设为失败
            result = False
            # 获取同步通道的参数
            channel_guid = channel_info.get("channel_guid")
            channel = channel_info.get("channel")
            name = channel_info.get("name")
            type = channel_info.get("type")
            try:
                type = TYPEBACK[type]
            except:
                yield logClient.tornadoErrorLog('({})通道({})的type类型不符合要求'.format(self.name, name))
                continue
            feedback = channel_info.get("feedback")
            try:
                feedback = TYPEBACK[feedback]
            except:
                yield logClient.tornadoErrorLog('({})通道({})的feedback类型不符合要求'.format(self.name,name))
                continue
            string_name = channel_info.get("string_name")
            min_value = channel_info.get("min_value")
            max_value = channel_info.get("max_value")
            raw_value = channel_info.get("raw_value")
            channel_value = channel_info.get("channel_value")
            level_value = channel_info.get("level_value")
            string_value = channel_info.get("string_value")
            matrix_value = channel_info.get('martix_value',{})

            #遍历本会议室设备的所有通道
            if channel_guid in my_channel_list:
                result = yield self.updateChannel(channel_guid, device_guid, device_name, device_port, channel, name,type, feedback, string_name, raw_value, min_value, max_value)
            #遍历后,没有找到该通道
            else:
                #判断是否有同一guid被删除过
                data = {
                    'database': self.company_db,
                    'fields': ['guid',],
                    'eq': {
                        'guid': channel_guid
                    },

                }
                msg = yield mysqlClient.tornadoSelectOnly('d_control_channel', data)
                if msg['ret'] != '0':
                    yield logClient.tornadoErrorLog('数据库查询错误:{}'.format('d_control_channel'))
                else:
                    #新增通道
                    if msg['lenght'] == 0:
                        result = yield self.addChannel(channel_guid,device_guid,device_name,device_port,channel,name,type,feedback,string_name,raw_value,min_value,max_value)
                    #更新通道
                    elif msg['lenght'] == 1:
                        result = yield self.updateChannel(channel_guid, device_guid,device_name, device_port, channel, name, type, feedback, string_name,raw_value, min_value, max_value)
            if result:
                # 保存同步的设备通道状态
                if type == 0 or type == 1:
                    self.ChannelUpdataStatus[channel_guid] = channel_value
                elif type == 2:
                    try:
                        self.ChannelUpdataStatus[channel_guid] = float(level_value)
                    except:
                        self.ChannelUpdataStatus[channel_guid] = float(min_value)
                elif type == 4:
                    self.ChannelUpdataStatus[channel_guid] = string_value
                elif type == 6:
                    if feedback == 0 or feedback == 1:
                        self.ChannelUpdataStatus[channel_guid] = channel_value
                    elif feedback == 2:
                        try:
                            self.ChannelUpdataStatus[channel_guid] = float(level_value)
                        except:
                            self.ChannelUpdataStatus[channel_guid] = float(min_value)
                    elif feedback == 4:
                        self.ChannelUpdataStatus[channel_guid] = string_value
                    elif feedback == 7:
                        self.ChannelUpdataStatus[channel_guid] = matrix_value
                    else:
                        yield logClient.tornadoErrorLog('未知通道:{}'.format(channel_guid))
                elif type == 7:
                    self.ChannelUpdataStatus[channel_guid] = matrix_value
                else:
                    yield logClient.tornadoErrorLog('未知通道:{}'.format(channel_guid))
        #删除的通道
        # for channel_guid_ in my_channel_list:
        #     for channel_info in channel_list:
        #         channel_guid = channel_info.get('channel_guid')
        #         if channel_guid_ == channel_guid:
        #             break
        #     else:
        #         yield self.deleteChannel(channel_guid_)

    # todo:同步-增加设备
    @gen.coroutine
    def addDevice(self,device_from,device_guid,device_name,device_port):
        data = {
            'database': self.company_db,
            'msg': {
                'guid': device_guid,
                'create_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),
                'is_delete': False,
                'name': device_name,
                'port': device_port,
                'meeting_room_guid_id': self.guid,
                'device_from':device_from,
                # 其他参数,未确定
                'standard': '',  # 规格
                'run_status': 3,  # 设备状态
                'authorization_status':1,
                'repair_date': 0,  # 保修月份
                'maintain_count': 0,  # 维修次数
                'change_count': 0,  # 更好次数
                'class_guid_id': '',  # 所属类别
                'brand_guid_id': '',  # 所属品牌
                'sn':'',
            }
        }
        msg = yield mysqlClient.tornadoInsertOne('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('({})数据库插入失败:{}'.format(self.name,'d_device'))
            return False
        yield logClient.tornadoDebugLog('({})增加设备({})'.format(self.name,device_name))
        return True

    #同步-更新设备
    @gen.coroutine
    def updateDevice(self,device_guid,device_name,device_port):
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'name': device_name,
                'port': device_port,
                'meeting_room_guid_id': self.guid,
                'is_delete': False,
            },
            'eq': {
                'guid': device_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_device'))
            return False
        yield logClient.tornadoDebugLog('({})更新设备({})'.format(self.name,device_name))
        return True

    # todo:同步-删除设备,设备通道
    @gen.coroutine
    def deleteDevice(self,device_guid):
        #1:删除设备
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'is_delete': True
            },
            'eq': {
                'guid': device_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库修改失败:{}'.format('d_device'))
            return
        yield logClient.tornadoWarningLog('({})删除设备({}),及所有通道'.format(self.name,device_guid))
        #2:删除设备所有通道
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'is_delete': True
            },
            'eq': {
                'device_guid_id': device_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库修改失败:{}'.format('d_control_channel'))

    #todo:同步-删除设备,设备通道
    #同步-增加通道
    @gen.coroutine
    def addChannel(self, channel_guid, device_guid, device_name, device_port, channel, name, type, feedback,string_name, raw_value, min_value, max_value):
        # string类型,判断string_name是否重名
        if type == 4:
            data = {
                'database': self.company_db,
                'fields': ['guid'],
                'eq': {
                    'string_name': string_name,
                    'meeting_room_guid_id': self.guid,
                    'is_delete': False
                },
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_control_channel', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_control_channel'))
                return False
            if msg['lenght'] >= 1:  # 查询成功,查询的个数为0
                yield logClient.tornadoErrorLog('({})通道({})的string_name已存在,不能重复定义'.format(self.name, name))
                return False
        data = {
            'database': self.company_db,
            'msg': {
                'guid': channel_guid,
                'create_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),
                'is_delete': False,
                'device_guid_id': device_guid,
                'meeting_room_guid_id': self.guid,
                'port': device_port,

                'channel': channel,
                'name': name,
                'type': type,
                'feedback': feedback,
                'string_name': string_name,
                'raw_value': raw_value,
                'min_value': min_value,
                'max_value': max_value,
                'channel_type_guid_id': '',
                'used': 0,
                'auto_back': 0,
                'use_event': '',
                'sync_delay': 0,
            },
        }
        msg = yield mysqlClient.tornadoInsertOne('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库插入失败:{}'.format('d_control_channel'))
            return False
        yield logClient.tornadoDebugLog('({})设备({})增加通道({})'.format(self.name, device_name, name))
        return True

    # todo:同步-更新通道
    @gen.coroutine
    def updateChannel(self, channel_guid, device_guid, device_name, device_port, channel, name, type, feedback,string_name, raw_value, min_value, max_value):
        # string类型,判断string_name是否重名
        if type == 4:
            data = {
                'database': self.company_db,
                'fields': ['guid'],
                'eq': {
                    'string_name': string_name,
                    'meeting_room_guid_id': self.guid,
                    'is_delete': False,
                },
                'neq': {
                    'guid': channel_guid
                }
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_control_channel', data)
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_control_channel'))
                return False
            if msg['lenght'] >= 1:  # 查询成功,查询的个数为0
                yield logClient.tornadoErrorLog('({})通道({})的string_name已存在,不能重复定义'.format(self.name, name))
                return False
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'is_delete': False,
                'device_guid_id': device_guid,
                'meeting_room_guid_id': self.guid,
                'port': device_port,

                'channel': channel,
                'name': name,
                'type': type,
                'feedback': feedback,
                'string_name': string_name,
                'raw_value': raw_value,
                'min_value': min_value,
                'max_value': max_value,
            },
            'eq': {
                'guid': channel_guid
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_control_channel'))
            return False
        # yield logClient.tornadoDebugLog('({})设备({})更新通道({})'.format(self.name, device_name, name))
        return True

    #同步-删除一条通道
    @gen.coroutine
    def deleteChannel(self,channel_guid):
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'is_delete': True
            },
            'eq': {
                'guid': channel_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_control_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库修改失败:{}'.format('d_control_channel'))
        yield logClient.tornadoWarningLog('({})删除通道({})'.format(self.name,channel_guid))

    # ******************************************************************************************************************#
    #todo:获取会议室所有设备信息,同步给welink
    def getSelfAllDeviceInfo(self):
        device_list = []
        for device_guid,device_object in self.device_dict.items():
            device_list.append(self.getDeviceInfo(device_object))
        return device_list

    #todo：获取设备信息（一个）
    def getDeviceInfo(self,device_object):
        device_info = {}
        device_info['device_guid'] = device_object.guid
        device_info['device_name'] = device_object.name
        device_info['port'] = device_object.port
        device_info['gateway_id'] = self.guid
        device_info['room_id'] = self.guid
        device_info['function_list'] = []
        for channel_guid in device_object.channel_guid_list:
            channel_object = self.channel_dict.get(channel_guid)
            if channel_object == None:
                continue
            device_info['function_list'].append(self.getChannelInfo(channel_object))
        else:
            return device_info

    #todo：获取通道信息（一条）
    def getChannelInfo(self,channel_object):
        channel_info = {}
        channel_info['channel_guid'] = channel_object.guid
        channel_info['channel'] = channel_object.channel
        channel_info['string_name'] = channel_object.string_name
        channel_info['name'] = channel_object.name
        channel_info['type'] = channel_object.type
        channel_info['feedback'] = channel_object.feedback
        if channel_info['type'] == 'string':
            channel_info['string_value'] = channel_object.string_value
        elif channel_info['type'] == 'level':
            channel_info['level_value'] = channel_object.level_value
            channel_info['max_value'] = channel_object.max_value
            channel_info['min_value'] = channel_object.min_value
            channel_info['raw_value'] = channel_object.raw_value
        else:
            try:
                channel_info['channel_value'] = channel_object.channel_value
            except:
                pass
        return channel_info

    @gen.coroutine
    def getDeviceChannelInfo(self,device_guid):
        channel_list = []
        if device_guid is None:
            for channel_guid, channel_object in self.channel_dict.items():
                channel_info = yield channel_object.getSelfInfo()
                channel_list.append(channel_info)
        else:
            for channel_guid, channel_object in self.channel_dict.items():
                if channel_object.device_guid_id == device_guid:
                    channel_info = yield channel_object.getSelfInfo()
                    channel_list.append(channel_info)
        return channel_list

    #******************************************************************************************************************#
    @gen.coroutine
    def meetingRoomInit(self):
        self.updating = True
        yield self.updateSelfControlChannel1()
        yield self.updateSelfDevice1()
        yield self.updateChannelToDevice()
        #后台编辑内容
        yield self.updateMacroChannel()
        yield self.initSelfScene()
        yield self.initAllMeetingRoomStatus()
        yield self.initAllRule()
        self.updating = False
        self.ioloop.add_timeout(self.ioloop.time(),self.handleMacroSync,0,{})
        # 获取会议室排程信息,会议室状态
        topic = '/aaiot/{}/receive/controlbus/event/schedule/schedule_info/0'.format(self.guid)
        yield self.myPublish(topic,'',qos=QOS_LOCAL)
        # 获取会议室排程剩余时间信息
        topic = '/aaiot/{}/receive/controlbus/event/schedule/remaining_time/0'.format(self.guid)
        yield self.myPublish(topic, '', qos=QOS_LOCAL)

    #todo:更新通道
    @gen.coroutine
    def updateSelfControlChannel1(self):
        self.channel_dict = {}
        # 获取设备控制通道
        data = {
            'database': self.company_db,
            'fields': ['guid',
                       'meeting_room_guid_id',
                       'device_guid_id',
                       'channel_type_guid_id',
                       'port',
                       'channel',
                       'name',
                       'string_name',
                       'type',
                       'feedback',
                       'raw_value',
                       'min_value',
                       'max_value',
                       'used',
                       'auto_back',
                       'use_event',
                       ],
            'eq': {
                'is_delete': False,
                'meeting_room_guid_id': self.guid,
            },
            # 'sortInfo': [
            #     {'update_time': ''},
            # ]
        }
        msg = yield mysqlClient.tornadoSelectAll('d_control_channel', data)
        try:
            if msg['ret'] != '0':
                yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_control_channel'))
                return
        except:
            pass
        controlChannels = msg['msg']
        for controlChannel in controlChannels:
            channel_ = ControlChannel(**controlChannel,receiveTopic=self.receiveTopic,sendTopic=self.sendTopic)
            self.channel_dict[channel_.guid] = channel_
            yield self.mosquittoObject.updateAllocateChannel(channel_)

    #todo:更新设备
    @gen.coroutine
    def updateSelfDevice1(self):
        self.device_dict = {}
        self.gatewayStatus = None
        data = {
            'database': self.company_db,
            'fields': ['guid',
                       'meeting_room_guid_id',
                       'class_guid_id',
                       'brand_guid_id',
                       'standard',
                       'port',
                       'run_status as status',
                       'repair_date',
                       'name',
                       'maintain_count',
                       'change_count'],
            'eq': {
                'is_delete': False,
                'meeting_room_guid_id': self.guid,
                'device_from':[0,1,2],
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_device'))
            return
        devices = msg['msg']
        for device in devices:
            # 获取设备
            device_ = Device(company_db=self.company_db,
                             meeting_room_object=self,
                            **device)
            self.device_dict[device_.guid] = device_

            yield self.awaitDeviceInit(device_)
            #设备在线
            online_channel_guid = device_.online_channel_guid
            #设备开关
            # self.on_info['channel_guid']
            # self.on_info['control_type']
            # self.on_info['realize_status']
            on_info = device_.on_info
            on_channel_guid = on_info.get('channel_guid')
            off_info = device_.off_info
            off_channel_guid = off_info.get('channel_guid')

            channel_object = self.channel_dict.get(online_channel_guid)
            if channel_object != None:
                yield logClient.tornadoWarningLog('会议室:({}),添加通道:({}),触发设备:({})的在线状态'.format(self.name,channel_object.name,device_.name))
                channel_object.updateCallback.append(device_.updateSelfOnlineStatus)

            channel_object = self.channel_dict.get(on_channel_guid)
            if channel_object != None:
                yield logClient.tornadoWarningLog('会议室:({}),添加通道:({}),触发设备:({})的开机状态'.format(self.name, channel_object.name, device_.name))
                channel_object.updateCallback.append(device_.updateSelfSwitchOnStatus)

            channel_object = self.channel_dict.get(off_channel_guid)
            if channel_object != None:
                yield logClient.tornadoWarningLog('会议室:({}),添加通道:({}),触发设备:({})的关机状态'.format(self.name, channel_object.name, device_.name))
                channel_object.updateCallback.append(device_.updateSelfSwitchOffStatus)

    @gen.coroutine
    def awaitDeviceInit(self,device_object):
        i = 0
        while device_object.init_status:
            yield gen.sleep(0.1)
            i += 1
            if i >= 400:
                break

    #todo:更新通道指向设备
    @gen.coroutine
    def updateChannelToDevice(self):
        self.monitorTopicToObject = {}
        for channel_object in self.channel_dict.values():
            # 获取控制通道的监听通道
            monitor_topic = channel_object.receiveTopic
            # 监听通道关联,控制通道对象
            if monitor_topic:
                self.monitorTopicToObject[monitor_topic] = channel_object

            channel_guid = channel_object.guid

            channel_device_guid = channel_object.device_guid_id
            if channel_device_guid:
                device_object = self.device_dict.get(channel_device_guid)
                if device_object:
                    device_object.channel_guid_list.append(channel_guid)

    #todo:更新巨集通道
    @gen.coroutine
    def updateMacroChannel(self,macro_channel_guid=None):
        self.macro_channel_info_list = []
        for channel_guid, channel_object in self.channel_dict.items():
            if channel_object.type != 'macro':
                continue
            result = yield self.getOneMacroChannelInfo(channel_guid)
            if result:
                self.macro_channel_info_list.append(result)

    @gen.coroutine
    def getOneMacroChannelInfo(self,macro_channel_guid):
        #找到聚集通道的巨集关系
        data = {
            'database': self.company_db,
            'fields': [ 'macro_channel_guid_id',
                       'control_channel_guid_id',
                       'name',
                       'value',
                       'after_wait',
                       'sort_index',
                       'target_status'],
            'eq': {
                'is_delete': False,
                'macro_channel_guid_id': macro_channel_guid
            },
            'sortInfo': [
                {'target_status':''},
                {'sort_index': ''},
            ]
        }
        msg = yield mysqlClient.tornadoSelectAll('d_macro_relation', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询错误:{}'.format('d_macro_relation'))
            return
        macro_relations = msg['msg']
        macro = {}
        macro['jobs'] = {
            'on': [],
            'off': []
        }
        macro['info'] = macro_channel_guid
        macro['feedbackCondition'] = ''
        for macro_relation in macro_relations:
            macro_info = {}
            #'on'状态聚集关系
            if macro_relation['target_status'] == 1:
                #只绑定通道guid,不绑定对象
                macro_info['channel_guid'] = macro_relation['control_channel_guid_id']
                macro_info['name'] = macro_relation['name']
                macro_info['value'] = macro_relation['value']
                macro_info['after_wait'] = macro_relation['after_wait']
                macro['jobs']['on'].append(macro_info)
            #'off'状态聚集关系
            elif macro_relation['target_status'] == 0:
                # 只绑定通道guid,不绑定对象
                macro_info['channel_guid'] = macro_relation['control_channel_guid_id']
                macro_info['name'] = macro_relation['name']
                macro_info['value'] = macro_relation['value']
                macro_info['after_wait'] = macro_relation['after_wait']
                macro['jobs']['off'].append(macro_info)
        else:
            return macro

    @gen.coroutine
    def updateOneMacroInfo(self,macro_channel_guid):
        result = yield self.getOneMacroChannelInfo(macro_channel_guid)
        if result:
            for macro_info in self.macro_channel_info_list:
                guid = macro_info.get('info')
                if guid == macro_channel_guid:
                    macro_info = result
                    break
            else:
                self.macro_channel_info_list.append(result)
        yield self.handleMacroSync(0,{})

    @gen.coroutine
    def deleteOneMacroInfo(self,macro_channel_guid):
        for macro_info in self.macro_channel_info_list:
            guid = macro_info.get('info')
            if guid == macro_channel_guid:
                delete_ = macro_info
                break
        else:
            return
        self.macro_channel_info_list.remove(delete_)
        yield self.handleMacroSync(0, {})

    def TopicToList(self,topic):
        topic_list = topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        return topic_list

    # ******************************************************************************************************************#

    #todo:通道状态更新,更新会议室状态
    @gen.coroutine
    def updateMeetingRoomStatus(self, status_guid, status_name, status):
        # 会议室状态同步
        msg = {
            "type": "status_feedback",
            "meeting_room_guid": self.guid,
            "status_guid": status_guid,
            'status_name': status_name,
            "status": status,
            "time": time.time(),  # 时间戳
            # "msg": 'self sync',
            "ret": 0,
        }
        topic = '/aaiot/{}/send/controlbus/event/websocket/status_feedback'.format(self.guid)
        yield self.myPublish(topic, json_dumps(msg), QOS_LOCAL)
        # return meeting_room_status_guid

    @gen.coroutine
    def initSelfScene(self):
        self.sceneDict = {}
        data = {
            'database': self.company_db,
            'fields': [
                'd_scene.guid',             #场景guid
                'd_scene.name',             #场景名称
                'd_meeting_room_scene.control_channel_guid_id as control_channel_guid', #场景关联聚集通道
            ],
            'eq':{
                'd_meeting_room_scene.meeting_room_guid_id':self.guid,
                'd_meeting_room_scene.is_delete':False,
                'd_scene.is_delete':False,
                'd_scene.guid':{'key':'d_meeting_room_scene.scene_guid_id'},
            }
        }
        msg = yield mysqlClient.tornadoSelectAll('d_scene,d_meeting_room_scene', data)
        if msg['ret'] == '0':
            scene_info_list = msg['msg']
        else:
            yield logClient.tornadoErrorLog('获取数据库:获取会议室场景失败')
            return
        for scene_info in scene_info_list:
            scene_object = Scene(**scene_info)
            self.sceneDict[scene_object.guid] = scene_object

    @gen.coroutine
    def initAllMeetingRoomStatus(self):
        self.meetingRoomStatusDict = {}
        db = self.company_db
        data = {
            'database': db,
            'fields': [
                # 'd_meeting_room_status.guid',
                'd_meeting_room_status.control_channel_guid_id',
                'd_status_name.name as status_name',
                'd_status_name.guid as status_name_guid',
                'd_status_name.on_name',
                'd_status_name.off_name',
                'd_status_name.unit'
            ],
            'eq': {
                'd_meeting_room_status.meeting_room_guid_id': self.guid,
                'd_meeting_room_status.status_name_guid_id': {'key': 'd_status_name.guid'},
                'd_meeting_room_status.is_delete': False,
            }
        }
        msg = yield mysqlClient.tornadoSelectAll('d_meeting_room_status,d_status_name', data)
        if msg['ret'] == '0':
            meeting_room_status_list = msg['msg']
        else:
            yield logClient.tornadoErrorLog('获取数据库:获取会议室状态失败')
            return
        for status_info in meeting_room_status_list:
            channel_guid = status_info.get('control_channel_guid_id')
            status_name = status_info.get('status_name')
            status_name_guid = status_info.get('status_name_guid')
            on_name = status_info.get('on_name')
            off_name = status_info.get('off_name')
            unit = status_info.get('unit')
            room_status_object = RoomStatus(guid=status_name_guid, name=status_name, on_name=on_name,off_name=off_name, unit=unit, meeting_room_object=self)

            self.meetingRoomStatusDict[status_name_guid] = room_status_object
            channel_object = self.channel_dict.get(channel_guid)
            if channel_object == None:
                continue
            # 添加通道更新任务   -- 更新房间状态
            yield logClient.tornadoWarningLog('会议室:({}),添加通道:({}).触发更新房间状态:({})'.format(self.name, channel_object.name, room_status_object.name))
            channel_object.updateCallback.append(room_status_object.updateSelfStatus)

    @gen.coroutine
    def initAllRule(self):
        self.rule_list = []
        db = self.company_db
        meeting_room_guid_list = self.guid
        data = {
            'database': db,
            'fields': [
                'guid',
                'meeting_room_guid_id as meeting_room_guid',
                'rule_handle',
                'device_guid_id as device_guid',
                'scene_guid_id as scene_guid',
                'execute_grade',
                'last_execute_time',
            ],
            'eq': {
                'is_delete': False,
                'meeting_room_guid_id': meeting_room_guid_list
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_rule_handle', data)
        if msg['ret'] == '0':
            rule_list = msg['msg']
        else:
            yield logClient.tornadoErrorLog('获取数据库:获取规则信息失败')
            return
        for rule_info in rule_list:
            #获取规则条件
            data = {
                'database': db,
                'fields': [
                    'guid',
                    'rule_handle_guid_id',
                    'trigger_type',
                    'compare_type',
                    'rule_time',
                    'repetition_type',
                    'status_name_guid_id as status_name_guid',
                    'keep_time',
                    'compare_value',
                    'schedule_status',
                    'after_time',
                    'before_time',
                    'recognition_type',
                    'device_sn',
                    'logic_index',
                ],
                'eq': {
                    'is_delete': False,
                    'rule_handle_guid_id': rule_info.get('guid')
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_rule_condition', data)
            if msg['ret'] == '0':
                rule_info['conditions'] = msg['msg']
            else:
                yield logClient.tornadoErrorLog('获取数据库:获取规则信息失败')
            rule_object = Rule(**rule_info)
            self.rule_list.append(rule_object)

        # 状态触发规则.分配给会议室状态更改时自动触发
        self.scheduleCallbackList = []
        self.timeEventCallbackList = []
        self.recognitionCallbackList = []
        time_callback = set()
        status_callback = {}
        schedule_callback = set()
        recognition_callback = set()
        for rule_object in self.rule_list:
            for execute_condition_list in rule_object.executeCondition:
                for execute_condition in execute_condition_list:
                    trigger_type = execute_condition.get('trigger_type')
                    if trigger_type == 0:   #时间触发
                        time_callback.add(rule_object)
                    elif trigger_type == 1: #状态触发
                        status_name_guid = execute_condition.get('status_name_guid')
                        if status_name_guid in status_callback.keys():
                            status_callback[status_name_guid].append(rule_object)
                        else:
                            status_callback[status_name_guid] = [rule_object]
                    elif trigger_type == 2: #排程触发
                        schedule_callback.add(rule_object)
                    elif trigger_type == 3: #识别触发
                        recognition_callback.add(rule_object)
                    else:
                        pass
        for rule_o in time_callback:
            function_info = {}
            function_info['function'] = self.timeTriggerRule
            function_info['kwargs'] = {'rule_object':rule_o}
            self.timeEventCallbackList.append(function_info)

        for rule_o in schedule_callback:
            function_info = {}
            function_info['function'] = self.scheduleTriggerRule
            function_info['kwargs'] = {'rule_object': rule_o}
            self.scheduleCallbackList.append(function_info)

        for rule_o in recognition_callback:
            function_info = {}
            function_info['function'] = self.recognitionTriggerRule
            function_info['kwargs'] = {'rule_object': rule_o}
            self.recognitionCallbackList.append(function_info)

        for status_name_guid,rule_object_list in status_callback.items():
            meeting_room_status_object = self.meetingRoomStatusDict.get(status_name_guid)
            if meeting_room_status_object == None:
                continue
            function_info = {}
            for rule_o in rule_object_list:
                function_info['function'] = self.statusTriggerRule
                function_info['kwargs'] = {'rule_object': rule_o}
                meeting_room_status_object.updateCallback.append(function_info)

    #todo:状态触发规则
    @gen.coroutine
    def statusTriggerRule(self,rule_object,status_value,update_time,auto_callback):
        # yield logClient.tornadoInfoLog('({}):执行状态触发规则'.format(self.name))
        i = 0
        valid_index = set()
        all_execute_condition = rule_object.executeCondition
        for execute_condition_list in all_execute_condition:
            for execute_condition in execute_condition_list:
                trigger_type = execute_condition.get('trigger_type')
                #定时触发情况在获取一次状态值
                if trigger_type == 1:
                    if auto_callback:
                        execute_condition['auto_callback_task'] = None
                        status_name_guid = execute_condition.get('status_name_guid')
                        meeting_room_status_object = self.meetingRoomStatusDict.get(status_name_guid)
                        if meeting_room_status_object == None:
                            continue
                        status_value = yield meeting_room_status_object.getStatusValue()
                    compare_type = execute_condition.get('compare_type')
                    keep_time = execute_condition.get('keep_time')
                    compare_value = execute_condition.get('compare_value')
                    if compare_type == 0:   #==
                        result = status_value == compare_value
                    elif compare_type == 1: #!=
                        result = status_value != compare_value
                    elif compare_type == 2: #>
                        result = status_value > compare_value
                    elif compare_type == 3: #>=
                        result = status_value >= compare_value
                    elif compare_type == 4: #<
                        result = status_value < compare_value
                    elif compare_type == 5: #<=
                        result = status_value <= compare_value
                    else:
                        return False

                    time_result = keep_time <= (time.time()-update_time)//60
                    if result == True and time_result == False:     #条件满足,时间不满足
                        auto_callback_task = execute_condition.get('auto_callback_task')
                        if auto_callback_task == None:
                            #没有定时查询任务,则添加任务
                            delay_time = keep_time*60+5-int((time.time()-update_time))
                            yield logClient.tornadoInfoLog('会议室:{},状态触发事件：状态条件满足,添加定时查询任务,{}秒后执行查询'.format(self.name,delay_time))
                            execute_condition['status'] = False
                            execute_condition['auto_callback_task'] = self.ioloop.add_timeout(self.ioloop.time()+delay_time,self.statusTriggerRule,rule_object,status_value,update_time,auto_callback=1)
                        else:
                            yield logClient.tornadoInfoLog('会议室:{},状态触发事件：存在相同任务,不添加查询任务'.format(self.name))
                    elif result == True and time_result == True:    #添加满足,时间也满足,状态为True
                        valid_index.add(i)
                        execute_condition['status'] = True
                        yield logClient.tornadoInfoLog('会议室:{},状态触发事件：条件满足,查看其他条件是否满足'.format(self.name))
                    else:
                        execute_condition['status'] = False
                        auto_callback_task = execute_condition.get('auto_callback_task')
                        if auto_callback_task:
                            yield logClient.tornadoInfoLog('会议室:{},状态触发事件：条件不满足,删除查询任务'.format(self.name))
                            self.ioloop.remove_timeout(auto_callback_task)
                            execute_condition['auto_callback_task'] = None
                        else:
                            yield logClient.tornadoInfoLog('会议室:{},状态触发事件：条件不满足,直接退出'.format(self.name))
            i += 1
        for index in valid_index:
            yield self.checkAllCondition(rule_object, index)

    #todo:识别触发规则
    @gen.coroutine
    def recognitionTriggerRule(self,type,user_id,user_type,user_permission,device_sn,rule_object):
        # yield logClient.tornadoInfoLog('({}):执行识别触发规则'.format(self.name))
        i = 0
        valid_index = set()
        all_execute_condition = rule_object.executeCondition
        for execute_condition_list in all_execute_condition:
            for execute_condition in execute_condition_list:
                trigger_type = execute_condition.get('trigger_type')
                if trigger_type == 3:  # 识别触发
                    recognition_type = execute_condition.get('recognition_type')
                    device_sn_s = execute_condition.get('device_sn')
                    if type == 'face':
                        type = 0
                    if type == recognition_type:    #识别类型相同
                        if device_sn_s == device_sn:
                            if user_type == 0:
                                valid_index.add(i)
                                execute_condition['status'] = True
                            else:
                                data = {
                                    'database': self.company_db,
                                    'fields': ['d_device_permission.guid'],
                                    'eq': {
                                        'd_device_permission.is_delete': False,
                                        'd_device.is_delete': False,
                                        'd_device.sn': device_sn,
                                        'd_device_permission.user_id_id': user_id,
                                        'd_device_permission.device_guid_id': {'key': 'd_device.guid'},
                                    },
                                }
                                msg = yield mysqlClient.tornadoSelectOnly('d_device,d_device_permission', data)
                                if msg['ret'] == '0' and msg['lenght'] >= 1:
                                    valid_index.add(i)
                                    execute_condition['status'] = True
                                else:
                                    #判断是否改用户具有整个会议室权限
                                    data = {
                                        'database': self.company_db,
                                        'fields': ['guid'],
                                        'eq': {
                                            'is_delete': False,
                                            'user_id_id': user_id,
                                            'meeting_room_guid_id': self.guid,
                                        }
                                    }
                                    msg = yield mysqlClient.tornadoSelectOnly('d_device_permission', data)
                                    if msg['ret'] == '0' and msg['lenght'] >= 1:
                                        valid_index.add(i)
                                        execute_condition['status'] = True
                                    else:
                                        execute_condition['status'] = False
                        else:
                            execute_condition['status'] = False
                    else:
                        execute_condition['status'] = False
            i += 1
        for index in valid_index:
            yield self.checkAllCondition(rule_object, index)

    #todo:排程触发规则
    @gen.coroutine
    def scheduleTriggerRule(self,schedule_status,schedule_time_info,rule_object):
        '''
        排程规则:

        开始状态:
            开始前
            开始后
        结束状态:
            结束后
        比较规则
        状态 ==
        时间 >=
        '''
        # yield logClient.tornadoInfoLog('({}):执行排程触发规则'.format(self.name))
        #开始前
        start_before_info = schedule_time_info.get('start_before')

        #开始后
        start_after_info = schedule_time_info.get('start_after')

        #结束后
        over_after_info = schedule_time_info.get('over_after')

        i = 0
        # valid_index = set()
        all_execute_condition = rule_object.executeCondition
        for execute_condition_list in all_execute_condition:
            for execute_condition in execute_condition_list:
                trigger_type = execute_condition.get('trigger_type')
                if trigger_type == 2:  # 查找时间触发
                    execute_schedule_status = execute_condition.get('schedule_status')
                    after_time = execute_condition.get('after_time')
                    before_time = execute_condition.get('before_time')

                    if execute_schedule_status == 0:    #开始前执行
                        if before_time:     #规则定义了开始前时间
                            #规则是会议开始前执行
                            if schedule_status == '空闲中' or schedule_status == '准备中':
                                if start_before_info != None:
                                    #获取延时
                                    # start_before_delay = execute_condition.get('start_before_delay')
                                    # if start_before_delay == None:
                                    #     start_before_delay = 0
                                    #获取排程信息
                                    start_before_time = start_before_info.get('time')
                                    start_before_schedule_guid = start_before_info.get('schedule_guid')

                                    # if start_before_time == before_time - start_before_delay or (execute_condition.get('start_before_schedule_guid') != start_before_schedule_guid and start_before_time <= before_time):
                                    if execute_condition.get('start_before_schedule_guid') != start_before_schedule_guid and start_before_time <= before_time:
                                        # valid_index.add(i)
                                        execute_condition['status'] = True
                                        yield logClient.tornadoInfoLog('会议室:({}),开始前规则：时间满足'.format(self.name))
                                        result = yield self.checkAllCondition(rule_object, i)
                                        if result == False: #
                                            yield logClient.tornadoInfoLog('会议室:({}),开始前规则满足,其他条件不满足')
                                            # if execute_condition.get('start_before_delay'):
                                            #     execute_condition['start_before_delay'] += 1
                                            #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,延时1分钟'.format(self.name))
                                            # else:
                                            #     execute_condition['start_before_delay'] = 1
                                            #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,继续延时1分钟'.format(self.name))
                                        else:
                                            yield logClient.tornadoInfoLog('会议室:({}),开始前规则满足,其他条件也满足,清楚延时,记录执行的排除guid'.format(self.name))
                                            # execute_condition['start_before_delay'] = 0
                                            execute_condition['start_before_schedule_guid'] = start_before_schedule_guid
                                    else:
                                        yield logClient.tornadoInfoLog('会议室:({}),开始前规则：时间不满足'.format(self.name))
                                        execute_condition['status'] = False
                            else:
                                execute_condition['status'] = False
                                yield logClient.tornadoInfoLog('会议室:({}),开始前规则：排程状态不满足'.format(self.name))
                        else:               #规则定义了开始前0分钟,获取开始后0分钟,开始后时间
                            #规则是正点
                            if schedule_status == '进行中':
                                if start_after_info != None:
                                    #获取延时
                                    # start_after_delay = execute_condition.get('start_after_delay')
                                    # if start_after_delay == None:
                                    #     start_after_delay = 0
                                    #获取排程信息
                                    start_after_time = start_after_info.get('time')
                                    start_after_schedule_guid = start_after_info.get('schedule_guid')

                                    # if start_after_time == before_time - start_after_delay or (execute_condition.get('start_after_schedule_guid') != start_after_schedule_guid and start_after_time >= before_time):
                                    if execute_condition.get('start_after_schedule_guid') != start_after_schedule_guid and start_after_time >= after_time:
                                        # valid_index.add(i)
                                        execute_condition['status'] = True
                                        yield logClient.tornadoInfoLog('会议室:({}),开始后规则：时间满足'.format(self.name))
                                        result = yield self.checkAllCondition(rule_object, i)
                                        if result == False: #
                                            yield logClient.tornadoInfoLog('会议室:({}),开始后规则满足,其他条件不满足')
                                            # if execute_condition.get('start_after_delay'):
                                            #     execute_condition['start_after_delay'] += 1
                                            #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,延时1分钟'.format(self.name))
                                            # else:
                                            #     execute_condition['start_after_delay'] = 1
                                            #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,继续延时1分钟'.format(self.name))
                                        else:
                                            yield logClient.tornadoInfoLog('会议室:({}),开始后规则满足,其他条件也满足,清楚延时,记录执行的排除guid'.format(self.name))
                                            # execute_condition['start_after_delay'] = 0
                                            execute_condition['start_after_schedule_guid'] = start_after_schedule_guid

                                    else:
                                        execute_condition['status'] = False
                                        yield logClient.tornadoInfoLog('会议室:({}),开始后规则：时间不满足'.format(self.name))
                            else:
                                execute_condition['status'] = False
                                yield logClient.tornadoInfoLog('会议室:({}),开始后规则：排程状态不满足'.format(self.name))
                    elif execute_schedule_status == 1:  #结束
                        #规则是会议结束后执行
                        if schedule_status == '空闲中':
                            if over_after_info != None:
                                #获取延时
                                # over_after_delay = execute_condition.get('over_after_delay')
                                # if over_after_delay == None:
                                #     over_after_delay = 0
                                #获取排程信息
                                over_after_time = over_after_info.get('time')
                                over_after_schedule_guid = over_after_info.get('schedule_guid')
                                # if over_after_time == before_time - over_after_delay or (execute_condition.get('over_after_schedule_guid') != over_after_schedule_guid and over_after_time >= before_time):
                                if execute_condition.get('over_after_schedule_guid') != over_after_schedule_guid and over_after_time >= after_time:
                                    # valid_index.add(i)
                                    execute_condition['status'] = True
                                    yield logClient.tornadoInfoLog('会议室:({}),结束后规则：时间满足'.format(self.name))
                                    result = yield self.checkAllCondition(rule_object, i)
                                    if result == False:  #
                                        yield logClient.tornadoInfoLog('会议室:({}),结束后规则满足,其他条件不满足'.format(self.name))
                                        # if execute_condition.get('over_after_delay'):
                                        #     execute_condition['over_after_delay'] += 1
                                        #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,延时1分钟'.format(self.name))
                                        # else:
                                        #     execute_condition['over_after_delay'] = 1
                                        #     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,继续延时1分钟'.format(self.name))
                                    else:
                                        yield logClient.tornadoInfoLog('会议室:({}),结束后规则满足,其他条件也满足,清楚延时,记录执行的排除guid'.format(self.name))
                                        # execute_condition['over_after_delay'] = 0
                                        execute_condition['over_after_schedule_guid'] = over_after_schedule_guid
                                else:
                                    execute_condition['status'] = False
                                    yield logClient.tornadoInfoLog('会议室:({}),结束后规则：时间不满足'.format(self.name))
                        else:
                            execute_condition['status'] = False
                            yield logClient.tornadoInfoLog('会议室:({}),结束后规则：排程状态不满足'.format(self.name))
            i += 1
        # for index in valid_index:
        #     result = yield self.checkAllCondition(rule_object,index)
        #     if result == False:
        #         for execute_condition in rule_object.executeCondition[index]:
        #             if execute_condition.get('trigger_type') == 2 and execute_condition.get('status') == True:
        #                 if execute_condition.get('delay'):
        #                     execute_condition['delay'] += 1
        #                     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,延时1分钟'.format(self.name))
        #                 else:
        #                     execute_condition['delay'] = 1
        #                     yield logClient.tornadoInfoLog('会议室:({}),其他条件不满足,继续延时1分钟'.format(self.name))

    # todo:时间触发规则
    @gen.coroutine
    def timeTriggerRule(self, now_time,rule_object):
        '''
        时间规则:
        等于设定时间时,条件有效,过时则条件无效,   (有效时间1分钟,无效时间24小时/分钟-1分钟)
        等于设定时间时,条件有效,过时则条件无效,   (有效时间1分钟,无效时间24小时/分钟-1分钟)
        不等于设定时间都有效,设定时间无效       (有效时间为24小时/分钟-1分钟,无效时间为1分钟)
        大于,大于等于,设定时间-00:00之间有效, (有效时间设定时间-00:00,无效时间00:00-设定时间)
        小于,小于等于,00:00-设定时间之间有效, (有效时间00:00-设定时间,无效时间设定时间-00:00)
        '''
        # yield logClient.tornadoInfoLog('({}):执行时间触发规则'.format(self.name))
        try:
            now_time_object = datetime.datetime.strptime(now_time, '%H:%M:%S')
        except:
            return
        i = 0
        valid_index = set()
        all_execute_condition = rule_object.executeCondition
        for execute_condition_list in all_execute_condition:
            for execute_condition in execute_condition_list:
                trigger_type = execute_condition.get('trigger_type')
                if trigger_type == 0:   #查找时间触发
                    #判断时间是否满足条件
                    rule_time = execute_condition.get('rule_time')
                    repetition_type = execute_condition.get('repetition_type')
                    compare_type = execute_condition.get('compare_type')
                    if rule_object.last_execute_time == None:
                        last_execute_time = None
                    else:
                        last_execute_time = rule_object.last_execute_time
                    now_time = datetime.datetime.now().replace(second=0,microsecond=0)
                    repetition_condition = yield self.repetitionCheck(last_execute_time,repetition_type,now_time.weekday())
                    if not repetition_condition:
                        execute_condition['status'] = False
                        i += 1
                        continue
                    result = False
                    rule_time_object = datetime.datetime.strptime(rule_time, '%H:%M:%S')
                    if compare_type == 0:                       #等于
                        result = now_time_object == rule_time_object
                    elif compare_type == 1:                     #不等于
                        result = now_time_object != rule_time_object
                    elif compare_type == 2:                     #大于
                        result = now_time_object > rule_time_object
                    elif compare_type == 3:                     #大于等于
                        result = now_time_object >= rule_time_object
                    elif compare_type == 4:                     #小于
                        result = now_time_object < rule_time_object
                    elif compare_type == 5:                     #小于等于
                        result = now_time_object <= rule_time_object
                    if result:
                        valid_index.add(i)
                        execute_condition['status'] = True
                    else:
                        execute_condition['status'] = False
                    i += 1
        for index in valid_index:
            yield self.checkAllCondition(rule_object, index)

    #todo:重复校验
    @gen.coroutine
    def repetitionCheck(self, last_execute_time, repetition_type, week):
        repetition_Flag = False
        # 一次
        if repetition_type == 0:
            repetition_Flag = True
        # 每天
        elif repetition_type == 1:
            repetition_Flag = True
        # 周一至周五
        elif repetition_type == 2:
            if week >= 0 and week <= 4:
                repetition_Flag = True
        # 周六至周日
        elif repetition_type == 3:
            if week >= 5 and week <= 6:
                repetition_Flag = True
        # 周一至周六
        elif repetition_type == 4:
            if week >= 0 and week <= 5:
                repetition_Flag = True
        # 周一
        elif repetition_type == 5:
            if week == 0:
                repetition_Flag = True
        # 周二
        elif repetition_type == 6:
            if week == 1:
                repetition_Flag = True
        # 周三
        elif repetition_type == 7:
            if week == 2:
                repetition_Flag = True
        # 周四
        elif repetition_type == 8:
            if week == 3:
                repetition_Flag = True
        # 周五
        elif repetition_type == 9:
            if week == 4:
                repetition_Flag = True
        # 周六
        elif repetition_type == 10:
            if week == 5:
                repetition_Flag = True
        # 周日
        elif repetition_type == 11:
            if week == 6:
                repetition_Flag = True

        if repetition_Flag:
            if last_execute_time == None or last_execute_time == '':
                return True
            else:
                if datetime.datetime.now().date() != last_execute_time.date():
                    return True

    #todo:检查所有其他条件
    @gen.coroutine
    def checkAllCondition(self, rule_object, index):
        for execute_condition in rule_object.executeCondition[index]:
            status = execute_condition.get('status')
            if not status:
                return False
        else:
            yield self.executeRule(rule_object)

    #更新上次执行时间
    @gen.coroutine
    def updateRuleLastExecuteTime(self, rule_object):
        data = {
            'database': self.company_db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'last_execute_time': datetime.datetime.now()
            },
            'eq': {
                'guid': rule_object.guid
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_rule_handle', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_rule'))
        else:
            rule_object.last_execute_time = datetime.datetime.now()

    # todo:规则执行
    @gen.coroutine
    def executeRule(self, rule_object):
        device_guid = rule_object.device_guid
        rule_handle = rule_object.rule_handle
        scene_guid = rule_object.scene_guid
        #   rule_handle == 1    控制设备开
        if rule_handle == 1:
            if device_guid == '':
                for device_guid, device_object in self.device_dict.items():
                    device_switch_info = device_object.on_info
                    if not device_switch_info or not device_switch_info.get('control_type'):
                        yield logClient.tornadoDebugLog('设备:({})的开机状态设备不完善,不能执行开机({})'.format(device_object.name,device_switch_info))
                        continue
                    yield self.controlDeviceSwitch(device_switch_info)
            else:
                device_object = self.device_dict.get(device_guid)
                if device_object != None:
                    device_switch_info = device_object.on_info
                    if not device_switch_info or not device_switch_info.get('control_type'):
                        yield logClient.tornadoDebugLog('设备:({})的开机状态设备不完善,不能执行开机({})'.format(device_object.name, device_switch_info))
                        return
                    yield self.controlDeviceSwitch(device_switch_info)
        #   rule_handle == 0    控制设备关
        #   rule_handle == 2    控制设备省电,
        elif rule_handle == 0 or rule_handle == 2:
            if device_guid == '':
                for device_guid, device_object in self.device_dict.items():
                    device_switch_info = device_object.off_info
                    if not device_switch_info or not device_switch_info.get('control_type'):
                        yield logClient.tornadoDebugLog(
                            '设备:({})的关机状态设备不完善,不能执行关机({})'.format(device_object.name, device_switch_info))
                        continue
                    else:
                        yield self.controlDeviceSwitch(device_switch_info)
            else:
                device_object = self.device_dict.get(device_guid)
                if device_object != None:
                    device_switch_info = device_object.off_info
                    if not device_switch_info or not device_switch_info.get('control_type'):
                        yield logClient.tornadoDebugLog(
                            '设备:({})的关机状态设备不完善,不能执行关机({})'.format(device_object.name, device_switch_info))
                    else:
                        yield self.controlDeviceSwitch(device_switch_info)
        #   rule_handle == 3   取消排程,
        elif rule_handle == 3:
            yield self.cancelSchedule()
        # rule == 4 执行场景
        elif rule_handle == 4:
            yield self.controlScene(scene_guid)
        #更新执行时间
        yield self.updateRuleLastExecuteTime(rule_object)

    #todo:控制设备  包含设备的开,关
    @gen.coroutine
    def controlDeviceSwitch(self, device_switch_info):
        '''
        device_info = {
                'channel_guid']:
                'control_type']:
                'realize_status':
                    }
        '''
        realize_status = device_switch_info.get('realize_status')
        channel_guid = device_switch_info.get('channel_guid')
        control_channel_object = self.channel_dict.get(channel_guid)
        if control_channel_object == None:
            yield logClient.tornadoWarningLog('会议室:({}),设备开关通道不存在'.format(self.name))
            return
        # 比较已经处理目标开关状态
        if control_channel_object.getSelfStatus() == realize_status:
            yield logClient.tornadoInfoLog('规则控制设备开关,但是设备亿处于目标状态,忽略操作')
        else:
            yield logClient.tornadoInfoLog('规则控制设备开关,但是设备未处于目标状态,通过指令控制')
            if control_channel_object.type == 'macro':
                if USE_MACRO:
                    send_topic = control_channel_object.sendTopic
                    control_type = device_switch_info.get('control_type')
                    yield self.myPublish(send_topic, str(control_type), qos=QOS)
                else:
                    yield self.executeMacroChannel(control_channel_object.guid,realize_status)
            else:
                send_topic = control_channel_object.sendTopic
                control_type = device_switch_info.get('control_type')
                yield self.myPublish(send_topic, str(control_type), qos=QOS)

    #todo:取消排程
    @gen.coroutine
    def cancelSchedule(self):
        # 获取会议室排程剩余时间信息
        topic = '/aaiot/{}/receive/controlbus/event/schedule/cancel/0'.format(self.guid)
        yield self.myPublish(topic, '', qos=QOS_LOCAL)

    #todo:执行排程
    @gen.coroutine
    def controlScene(self, scene_guid):
        scene_object = self.sceneDict.get(scene_guid)
        if scene_object == None:
            return
        control_channel_guid = scene_object.control_channel_guid
        control_channel_object = self.channel_dict.get(control_channel_guid)
        if control_channel_object == None:
            return
        topic = control_channel_object.sendTopic
        if control_channel_object.type == 'macro':
            if USE_MACRO:
                event_type = yield control_channel_object.getSelfControlEvent(goal_state=1)
                if event_type:
                    yield self.myPublish(topic, event_type, qos=QOS)
                    yield logClient.tornadoInfoLog('({}):通过通道{},事件为:{},执行场景:{}'.format(self.name, control_channel_object.name, event_type,scene_object.name))
            else:
                yield self.executeMacroChannel(control_channel_object.guid,1)
        else:
            event_type = yield control_channel_object.getSelfControlEvent(goal_state=1)
            if event_type:
                yield self.myPublish(topic, event_type, qos=QOS)
                yield logClient.tornadoInfoLog('({}):通过通道{},事件为:{},执行场景:{}'.format(self.name, control_channel_object.name, event_type,scene_object.name))

    @gen.coroutine
    def executeMacroChannel(self,macro_channel_guid,goal_state):
        '''
        :param macro_channel_guid:  聚集通道guid
        :param goal_state:          聚集通道目标状态
        :return:
        '''
        for macro_channel_info in self.macro_channel_info_list:
            if macro_channel_info.get('info') == macro_channel_guid:
                if goal_state == 0 or goal_state == 'off':
                    channel_list = macro_channel_info.get('jobs').get('off')
                elif goal_state == 1 or goal_state == 'on':
                    channel_list = macro_channel_info.get('jobs').get('on')
                else:
                    return
                for channel_info in channel_list:
                    channel_guid = channel_info.get('channel_guid')
                    value = channel_info.get('value')
                    try:
                        value = float(value)
                    except:
                        pass
                    after_wait = channel_info.get('after_wait')
                    channel_object = self.channel_dict.get(channel_guid)
                    if channel_object == None:
                        continue
                    if channel_object.getSelfStatus() != value:
                        topic = channel_object.sendTopic
                        if channel_object.type == 'button':
                            event_type = 'click'
                        elif channel_object.type == 'macro':
                            yield self.executeMacroChannel(channel_object.guid,value)
                            continue
                        else:
                            event_type = value
                        yield self.myPublish(topic, event_type, qos=QOS)
                    if after_wait:
                        yield gen.sleep(after_wait/1000)