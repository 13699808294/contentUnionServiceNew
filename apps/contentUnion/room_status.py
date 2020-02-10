import time

from tornado import gen
from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps

class RoomStatus(object):
    def __init__(self,guid,name,on_name,off_name,unit,meeting_room_object):
        self.guid = guid
        self.name = name
        self.on_name = on_name
        self.off_name = off_name
        self.unit = unit
        self.meeting_room_object = meeting_room_object
        self.status = ''
        self.update_time = time.time()
        self.updateCallback = []

    @gen.coroutine
    def updateSelfStatus(self,channel_guid,status):
        self.status = status
        self.update_time = time.time()

        if self.on_name and self.status == 'on':
            string_status = self.on_name
        elif self.off_name and self.status == 'off':
            string_status = self.off_name
        else:
            string_status = self.status

        if self.unit:
            string_status += ' {}'.format(self.unit)
        yield logClient.tornadoInfoLog('会议室:{}的房间状态:{},更新:{}'.format(self.meeting_room_object.name,self.name,string_status))
        #同步会议室状态值
        yield self.meeting_room_object.updateMeetingRoomStatus(self.guid,self.name,string_status)
        # 状态触发规则
        for callback_info in self.updateCallback:
            function = callback_info.get('function')
            if function == None:
                continue
            selfStatus = yield self.getStatusValue()
            yield function(**callback_info.get('kwargs'), status_value= selfStatus, update_time=self.update_time,auto_callback=0)

    @gen.coroutine
    def getStatusValue(self):
        return self.status

    def getStringValue(self):
        if self.on_name and self.status == 'on':
            string_status = self.on_name
        elif self.off_name and self.status == 'off':
            string_status = self.off_name
        else:
            string_status = str(self.status)
        if self.unit:
            string_status += ' {}'.format(str(self.unit))
        return string_status