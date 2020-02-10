import json

from tornado import gen

from setting.setting import COMMANDBAKC
from utils.baseAsync import BaseAsync
from utils.logClient import logClient

class ControlChannel(BaseAsync):
    def __init__(self,
                 guid,
                 meeting_room_guid_id,
                 device_guid_id,
                 channel_type_guid_id,
                 port,
                 channel,
                 name,
                 string_name,
                 type,
                 feedback,
                 raw_value,
                 min_value,
                 max_value,
                 used,
                 auto_back,
                 use_event,
                 receiveTopic,
                 sendTopic):
        super().__init__()
        self.guid = guid
        self.meeting_room_guid_id = meeting_room_guid_id
        self.device_guid_id = device_guid_id
        self.channel_type_guid_id = channel_type_guid_id
        self.channel = channel
        self.port = port
        self.string_name = string_name
        self.name = name

        self.type = COMMANDBAKC[type]

        if feedback:
            self.feedback = COMMANDBAKC[feedback]
        else:
            self.feedback = 'none'

        self.raw_value = float(raw_value)
        self.min_value = float(min_value)
        self.max_value = float(max_value)
        self.used = used
        self.auto_back = auto_back

        self.use_event = use_event.split(',')
        while '' in self.use_event:
            self.use_event.remove('')

        self.baseReceiveTopic = receiveTopic
        self.baseSendTopic = sendTopic

        #更新发送和接收主题
        self.updateSelfTopic()
        #
        self.channel_value = None
        self.level_value = None
        self.string_value = None
        #发送主题
        self.onceClickBign = False
        self.keepBignStatus = None
        self.initSelfStatus()

        #
        self.control_log_list = []
        #
        self.updateCallback = []
    def updateSelfTopic(self):
        '''
        info = {
                'user_id':user_id,
                'control_event':control_event,
                'now_status':now_status,
                'back_status':content.get('status')
            }
        '''
        #控制
        if self.type == 'string':  # 判断type
            self.sendTopic = '{}/event/{}/{}/{}'.format(self.baseSendTopic, self.type, str(self.port),self.string_name)
        else:
            self.sendTopic = '{}/event/{}/{}/{}'.format(self.baseSendTopic, self.type, str(self.port),str(self.channel))  # 使用channel

        #反馈
        if self.feedback != '' and self.feedback != 'none' and self.feedback != None:
            if self.feedback == 'string':  # 判断type
                self.receiveTopic = '{}/event/{}/{}/{}'.format(self.baseReceiveTopic, self.feedback, str(self.port),self.string_name)
            else:
                self.receiveTopic = '{}/event/{}/{}/{}'.format(self.baseReceiveTopic, self.feedback, str(self.port),str(self.channel))  # 使用channel
        else:
            self.receiveTopic = ''
        # logClient.debugLog('通道:{}的控制主题为:{},监控主题为:{}'.format(self.name,self.sendTopic,self.receiveTopic))
    #通道控制
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
            'control_type':'控制类型'
            'input_port':'连接的输入源guid
            'connection_token':'连接token‘
        }
        '''
        event_type = msg.get('event_type')
        status = msg.get('status')
        back_status = None
        command_flag = 0
        #todo:非控制类通道,或者查询状态
        # if not self.type or not event_type:
        if not self.type or event_type == '' or event_type == None:
            now_status = self.getSelfStatus()
            content = {
                'ret': 0,
                'guid': self.guid,
                'status':now_status,
            }
            return content

        #todo:button类型事件
        elif self.type == 'button':
            #click 事件
            if event_type == 'click':
                if status == 'on':
                    back_status = 'off'
                    command_flag = 1
                elif status == 'off':
                    back_status = 'on'
                    command_flag = 1

            elif event_type == 'push':
                # 如果是自动返回状态通道
                if self.auto_back:
                    back_status = 'on'
                    self.channel_value = 'on'
                    command_flag = 1
                else:
                    if status == 'on':
                        back_status = 'off'
                        command_flag = 1
                    elif status == 'off':
                        back_status = 'on'
                        command_flag = 1
            elif event_type == 'release':
                # 如果是自动返回状态通道
                if self.auto_back:
                    back_status = 'off'
                    self.channel_value = 'off'
                    # command_flag = 1

        #todo:channel类型事件
        elif self.type == 'channel':
            if event_type == 'off':
                command_flag = 1
                back_status = 'off'
            elif event_type == 'on':
                command_flag = 1
                back_status = 'on'
            elif event_type == 'pulse':
                pass

        #todo:level类型事件
        elif self.type == 'level':
            try:
                event_type = float(event_type)
            except:
                event_type = self.min_value
            if event_type < self.min_value:
                event_type = self.min_value
            elif event_type > self.max_value:
                event_type = self.max_value
            back_status = event_type
            # if self.getSelfStatus() != back_status:
            command_flag = 1
            # else:
            #     command_flag = 0

        #todo:macro类型事件
        elif self.type == 'macro':
            if event_type == 'toggle':
                if status == 'off':
                    command_flag = 1
                    back_status = 'on'
                elif status == 'on':
                    command_flag = 1
                    back_status = 'off'
            elif event_type == 'on':
                if self.getSelfStatus() != 'on':
                    event_type = 'on'
                    back_status = 'on'
                    command_flag = 1
            elif event_type == 'off':
                if self.getSelfStatus() != 'off':
                    event_type = 'off'
                    back_status = 'off'
                    command_flag = 1

        #todo:matrix类事件
        elif self.type == self.feedback == 'matrix':
            input_port = msg.get('input_port')
            if input_port == None:
                content = {
                'ret': 1,
                }
                return content
            #断开输入源连接
            # if status == 'on':
            #     back_status = ''
            #     command_flag = 1
            # elif status == 'off':
            #     back_status = input_port
            #     command_flag = 1
            #没有切断功能
            back_status = input_port
            command_flag = 1
        #todo:commamd类事件
        elif self.type == 'command':
            back_status = status
            command_flag = 1
        elif self.type == 'string':
            back_status = status
            command_flag = 1
        content = {
            'ret': 0,
            'guid': self.guid,
            'status': back_status,
            'event_type':event_type,
            'command_flag':command_flag
        }
        return content

    #更新通道状态
    @gen.coroutine
    def updataSelfStatus(self,status):
        if self.type == 'button' or self.type == 'channel':
            if status == 'on' or status == 'off':
                self.channel_value = status
        elif self.type == 'level':
            try:
                self.level_value = float(status)
            except:
                pass
        elif self.type == 'string':
            self.string_value = status
        elif self.type == 'macro':
            if status == 'on' or status == 'off':
                self.channel_value = status
            else:
                yield logClient.tornadoInfoLog('聚集通道狀態反饋錯誤:{}'.format(status))
        elif self.type == 'matrix':
            if isinstance(status,dict):
                self.matrix_value = status.get('channel_guid')
                yield logClient.tornadoDebugLog('矩阵通道:{}更新状态为:{}'.format(self.name, self.matrix_value))
            else:
                try:
                    status = json.loads(status)
                    self.matrix_value = status.get('channel_guid')
                    yield logClient.tornadoDebugLog('矩阵通道:{}更新状态为:{}'.format(self.name,self.matrix_value))
                except:
                    yield logClient.tornadoErrorLog(status)
        elif self.type == 'none':
            if self.feedback == 'button' or self.feedback == 'channel':
                if status == 'on' or status == 'off':
                    self.channel_value = status
            elif self.feedback == 'level':
                try:
                    self.level_value = float(status)
                except:
                    pass
            elif self.feedback == 'string':
                self.string_value = status
            elif self.feedback == 'matrix':
                if isinstance(status, dict):
                    self.matrix_value = status.get('channel_guid')
                    yield logClient.tornadoDebugLog('矩阵通道:{}更新状态为:{}'.format(self.name, self.matrix_value))
                else:
                    try:
                        status = json.loads(status)
                        self.matrix_value = status.get('channel_guid')
                        yield logClient.tornadoDebugLog('矩阵通道:{}更新状态为:{}'.format(self.name, self.matrix_value))
                    except:
                        yield logClient.tornadoErrorLog(status)
            else:
                yield logClient.tornadoErrorLog('未知通道:{}'.format(self.guid))
                return
        else:
            yield logClient.tornadoErrorLog('未知通道:{}'.format(self.guid))
            return
        for callback in self.updateCallback:
            yield callback(self.guid,self.getSelfStatus())

    #获取当前通道状态
    def getSelfStatus(self):
        if self.type == 'button' or self.type == 'channel':
            return self.channel_value
        elif self.type == 'level':
            return float(self.level_value)
        elif self.type == 'string':
            return self.string_value
        elif self.type == 'macro':
            return self.channel_value
        elif self.type == 'matrix':
            return self.matrix_value
        else:
            if self.feedback == 'button' or self.feedback == 'channel':
                return self.channel_value
            elif self.feedback == 'level':
                return float(self.level_value)
            elif self.feedback == 'string':
                return self.string_value
            elif self.feedback == 'matrix':
                return self.matrix_value

    #初始化通道状态
    def initSelfStatus(self):
        if self.type == 'button' or self.type == 'channel':
            self.channel_value = 'off'
        elif self.type == 'level':
            self.level_value = self.raw_value
            if self.level_value > self.max_value or self.level_value < self.min_value:
                self.level_value = self.min_value
        elif self.type == 'string':
            self.string_value = ''
        elif self.type == 'matrix':
            self.matrix_value = None
        else:
            if self.feedback == 'button' or self.feedback == 'channel':
                self.channel_value = 'off'
            elif self.feedback == 'level':
                self.level_value = self.raw_value
                if self.level_value > self.max_value or self.level_value < self.min_value:
                    self.level_value = self.min_value
            elif self.feedback == 'string':
                self.string_value = ''
            elif self.feedback == 'matrix':
                self.matrix_value = None

    @gen.coroutine
    def getSelfInfo(self):
        info = {
            'meeting_room_guid': self.meeting_room_guid_id,
            'guid':self.guid,
            'type':self.type,
            'feedback':self.feedback,
            'name':self.name,
            'string_name':self.string_name,
            'raw_value':self.raw_value,
            'max_value':self.max_value,
            'min_value':self.min_value,
            'status':self.getSelfStatus(),
            'channel_type_guid_id':self.channel_type_guid_id,
            'port':self.port,
            'channel':self.channel,
            'device_guid':self.device_guid_id,
            'use_event':self.use_event,
        }
        return info

    @gen.coroutine
    def getSelfControlEvent(self,goal_state):
        if goal_state == 0 or goal_state == 'off':      #目标 关
            if self.type == 'button':
                if self.getSelfStatus() == 'on':
                    return 'click'
                else:
                    return
            elif self.type == 'channel':
                if self.getSelfStatus() == 'on':
                    return 'off'
                else:
                    return
            elif self.type == 'level':
                if self.getSelfStatus() != self.min_value:
                    return self.min_value
                else:
                    return
            elif self.type == 'macro':
                if self.getSelfStatus() == 'on':
                    return 'off'
                else:
                    return
        elif goal_state == 1 or goal_state == 'on': #目标 开
            if self.type == 'button':
                if self.getSelfStatus() == 'off':
                    return 'click'
                else:
                    return
            elif self.type == 'channel':
                if self.getSelfStatus() == 'off':
                    return 'on'
                else:
                    return
            elif self.type == 'level':
                if self.getSelfStatus() != self.max_value:
                    return self.max_value
                else:
                    return
            elif self.type == 'macro':
                if self.getSelfStatus() == 'off':
                    return 'on'
                else:
                    return
        else:
            return