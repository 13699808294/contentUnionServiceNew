import asyncio
import datetime
import uuid

from tornado import gen

from setting.setting import COMMANDBAKC
from utils.MysqlClient import mysqlClient
from utils.baseAsync import BaseAsync
from utils.logClient import logClient
from utils.my_json import json_dumps
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

class Device(BaseAsync):
    def __init__(self,guid,
                 meeting_room_guid_id,
                 class_guid_id,
                 brand_guid_id,
                 standard,
                 port,
                 status,
                 repair_date,
                 name,
                 maintain_count,
                 change_count,
                 company_db,
                 meeting_room_object,
                 ):
        super().__init__()
        self.guid = guid
        self.meeting_room_guid_id = meeting_room_guid_id
        self.class_guid_id = class_guid_id
        self.brand_guid_id = brand_guid_id
        self.standard = standard
        self.port = port
        self.status = status
        self.repair_date = repair_date
        self.name = name
        self.maintain_count = maintain_count
        self.change_count = change_count
        self.company_db = company_db

        self.meeting_room_object = meeting_room_object
        self.meeting_room_name = self.meeting_room_object.name


        self.switch_status = 0
        self.on_info = {}
        self.off_info = {}

        self.online_channel_guid = None
        self.online_status = None

        self.channel_guid_list = []
        self.init_status = True
        # self.ioloop.add_timeout(self.ioloop.time(),self.getSelfSwitchFunction)
        # self.ioloop.add_timeout(self.ioloop.time(), self.getSelfOnlineFunction)
        # self.ioloop.add_timeout(self.ioloop.time(),self.checkSelfOtherAttribute)

        task = self.selfInit()
        asyncio.ensure_future(task,loop=self.aioloop).add_done_callback(self.selfInitCallback)

        # futu = asyncio.ensure_future(do_some_work(3))
        # futu.add_done_callback(done_callback1)

    @gen.coroutine
    def selfInit(self):
        yield self.getSelfSwitchFunction()
        yield self.getSelfOnlineFunction()
        yield self.checkSelfOtherAttribute()


    def selfInitCallback(self,futu):
        self.init_status = False

    #todo:检查设备其他属性
    @gen.coroutine
    def checkSelfOtherAttribute(self):
        if not self.class_guid_id:
            yield logClient.tornadoWarningLog('会议室:({}),设备({})没有指定类别'.format(self.meeting_room_name, self.name))
        if not self.brand_guid_id:
            yield logClient.tornadoWarningLog('会议室:({}),设备({})没有指定品牌'.format(self.meeting_room_name, self.name))


    # todo:更新存储设备离线状态
    @gen.coroutine
    def updateSelfOnlineStatus(self, channel_guid, status):
        # if self.online_channel_guid == None:
        #     return
        # elif channel_guid != self.online_channel_guid:
        #     return
        # elif status not in ['on', 'off']:
        #     return
        # else:
        #     self.online_status = status
        #检查状态值有效性
        # if status not in ['on', 'off']:
        #     return
        self.online_status = status
        yield logClient.tornadoInfoLog('{}:设备({})在线状态更新为:{}'.format(self.meeting_room_name,self.name,status))
        yield self.meeting_room_object.updateDeviceOnlineStatus(self)
        if self.online_status == 'off':  # 掉线
            # 判断是否有离线记录,有则不添加
            data = {
                'database': self.company_db,
                'fields': ['guid'],
                'eq': {
                    'is_delete': False,
                    'device_guid_id': self.guid,
                    'status': True
                },
                'sortInfo': [
                    {'update_time': 'ASC'},
                ],
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_device_offline_record', data)
            if msg['ret'] == '0':
                if msg['lenght'] == 0:
                    # 添加离线记录
                    info = {
                        'device_guid_id': self.guid,
                        'status': True
                    }
                    yield self.mysqlStorage('d_device_offline_record', info)
            if self.off_info == {}:
                yield self.updateSelfSwitchOffStatus(None, 'off', no_channel=1)
        else:  # 上线
            # 判断是否有掉线记录,有则删除
            data = {
                'database': self.company_db,
                'fields': ['guid'],
                'eq': {
                    'is_delete': False,
                    'device_guid_id': self.guid,
                    'status': True
                },
                'sortInfo': [
                    {'update_time': 'ASC'},
                ],
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_device_offline_record', data)
            if msg['ret'] == '0':
                if msg['lenght'] > 0:
                    last_guid = msg['msg'][0]['guid']
                    # 更新上一次离线记录
                    data = {
                        'database': self.company_db,
                        'msg': {
                            'update_time': datetime.datetime.now(),
                            'status': False
                        },
                        'eq': {
                            'guid': last_guid
                        }
                    }
                    msg = yield mysqlClient.tornadoUpdateMany('d_device_offline_record', data)
                    if msg['ret'] == '0':
                        yield self.meeting_room_object.updateDeviceOnlineStatus(self)
                    else:
                        yield logClient.tornadoErrorLog('修改设备离线记录失败')
                else:
                    # 之前没有离线记录,上线记录不做操作
                    pass
            else:
                yield logClient.tornadoErrorLog('查询设备离线记录失败')
            if self.on_info == {}:
                yield self.updateSelfSwitchOnStatus(None,'on',no_channel=1)

    #todo:通过通道状态,更新设备开关状态
    @gen.coroutine
    def updateSelfSwitchOnStatus(self,channel_guid,status,no_channel=None):
        if channel_guid == self.on_info.get('channel_guid'):
            #判断是否开机
            if self.switch_status != 1:
                if no_channel:
                    if status == 'on':
                        self.switch_status = 1
                        yield self.meeting_room_object.updateDeviceSwitchStatus(self)
                        yield logClient.tornadoInfoLog(
                            '会议室:({}),设备({}),状态更新为开机'.format(self.meeting_room_name, self.name))
                        yield self.storageDeviceOpen()
                else:
                    if status == self.on_info.get('realize_status'):
                        self.switch_status = 1
                        yield self.meeting_room_object.updateDeviceSwitchStatus(self)
                        yield logClient.tornadoInfoLog('会议室:({}),设备({}),状态更新为开机'.format(self.meeting_room_name,self.name))
                        yield self.storageDeviceOpen()

    @gen.coroutine
    def updateSelfSwitchOffStatus(self, channel_guid, status,no_channel=None):
        if channel_guid == self.off_info.get('channel_guid'):
            #判断是否关机
            if self.switch_status != 0:
                if no_channel:
                    if status == 'off':
                        self.switch_status = 0
                        yield self.meeting_room_object.updateDeviceSwitchStatus(self)
                        yield logClient.tornadoInfoLog(
                            '会议室:({}),设备({}),状态更新为关机'.format(self.meeting_room_name, self.name))
                        yield self.storageDeviceClose()
                else:
                    if status == self.off_info.get('realize_status'):
                        self.switch_status = 0
                        yield self.meeting_room_object.updateDeviceSwitchStatus(self)
                        yield logClient.tornadoInfoLog('会议室:({}),设备({}),状态更新为关机'.format(self.meeting_room_name, self.name))
                        yield self.storageDeviceClose()

    #todo:每日更新
    @gen.coroutine
    def dayUpdateSelfOnSwitch(self):
        if self.switch_status == 0:
            return
        yield self.storageDeviceClose()
        yield gen.sleep(2)
        yield self.storageDeviceOpen()

    #todo:更新设备运行状态
    @gen.coroutine
    def updateSelfRunStatus(self):
        '''
        status
        0   运行中 -- 在线 +
        1   未连接 -- 不在线
        2   关闭中
        3   待修中
        '''
        if self.online_status == 0:
            status = 1
        elif self.switch_status == 0:     #关
            status = 2
        elif self.switch_status == 1:   #开
            status = 0
        else:
            return
        data = {
            'database':self.company_db,
            'msg':{
                'run_status':status
            },
            'eq':{
                'guid':self.guid,
                'is_delete':False,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_device', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_device'))

    #todo:记录开机
    @gen.coroutine
    def storageDeviceOpen(self):
        # 查询最近一次记录
        data = {
            'database': self.company_db,
            'fields': ['update_time', 'device_guid_id', 'status_name', 'work_time'],
            'eq': {
                'is_delete': False,
                'device_guid_id': self.guid,
            },
            'sortInfo': [
                {
                    'update_time': 'DESC',
                },
            ]
        }
        msg = yield mysqlClient.tornadoSelectOne('d_device_switch_record', data)
        if msg['ret'] == '0':
            if not msg['msg']:  # 没有记录开机,添加记录开机
                yield self.mysqlStorage('d_device_switch_record', {'status_name': 1, 'work_time': 0})
            elif msg['msg'].get('status_name') == 0:  # 上一次记录是关机,则记录一次开机
                yield self.mysqlStorage('d_device_switch_record', {'status_name': 1, 'work_time': 0})
        else:
            yield logClient.tornadoErrorLog('数据库查询错误:{}'.format('d_device_switch_record'))
            return

    @gen.coroutine
    def storageDeviceClose(self):
        # 关机需要统计本次开机时间,查询上一次的操作时间,并且需要开机才有效
        data = {
            'database': self.company_db,
            'fields': ['update_time', 'device_guid_id', 'status_name', 'work_time'],
            'eq': {
                'is_delete': False,
                'device_guid_id': self.guid,
            },
            'sortInfo': [{'update_time': 'DESC',},]
        }
        msg = yield mysqlClient.tornadoSelectOne('d_device_switch_record', data)
        if msg['ret'] == '0':
            if not msg['msg']:  # 没有任何记录,只记录一次关机,工作时间为0
                yield self.mysqlStorage('d_device_switch_record', {'status_name': 0, 'work_time': 0})
            elif msg['msg'].get('status_name') == 1:  # 上一次记录为开机,记录总工作时间
                record_time = msg['msg'].get('update_time')  # 上次开机时间
                now_time = datetime.datetime.now()  # 当前时间
                # 日期想同,记录一次统计信息
                if now_time.date() == datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S").date():
                    work_time = (now_time - datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S")).seconds
                    yield self.mysqlStorage('d_device_switch_record', {'status_name': 0, 'work_time': work_time})
                # 日期不相同,分天记录统计信息
                else:
                    # 计算上次记录当天剩余时间
                    once_time = datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S").replace(microsecond=0,hour=23, minute=59,second=59)
                    work_time = (once_time - datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S")).seconds
                    yield self.mysqlStorage('d_device_switch_record', {'status_name': 0, 'work_time': work_time},time=once_time)

                    start_time = (datetime.datetime.strptime(record_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=1)).replace(microsecond=0, hour=0, minute=0, second=0)
                    while True:
                        if now_time.date() <= start_time.date():
                            work_time = (now_time - start_time).seconds
                            yield self.mysqlStorage('d_device_switch_record',{'status_name': 0, 'work_time': work_time})
                            break
                        else:
                            work_time = 86400
                            yield self.mysqlStorage('d_device_switch_record',{'status_name': 0, 'work_time': work_time},time=datetime.datetime.strftime(start_time.replace(microsecond=0, hour=23, minute=59,second=59), "%Y-%m-%d %H:%M:%S"))
                            start_time = (start_time + datetime.timedelta(days=1))
        else:
            yield logClient.tornadoErrorLog('数据库查询错误:{}'.format('d_device_switch_record'))

    # todo:获取设备开关功能
    @gen.coroutine
    def getSelfSwitchFunction(self):
        data = {
            'database': self.company_db,
            'fields': ['guid',
                       'device_guid_id',            #所属设备
                       'control_channel_guid_id',   #所属控制通道
                       'function_name',             #功能名称 1 == 开机  0 == 关机
                       'control_type',              #控制方式
                       'realize_status',            #实现状态
                       ],
            'eq': {
                'is_delete': False,
                'device_guid_id': self.guid,
            },
        }
        msg = yield mysqlClient.tornadoSelectAll('d_device_switch_function', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_device_switch_function'))
            return
        else:
            switch_function_list = msg['msg']

        for switch_function_info in switch_function_list:
            function_name = switch_function_info.get('function_name')
            control_type = switch_function_info.get('control_type')
            realize_status = switch_function_info.get('realize_status')
            channel_guid = switch_function_info.get('control_channel_guid_id')
            if function_name == 1:
                #开机
                if self.on_info.get('channel_guid') != None:
                    yield logClient.tornadoWarningLog('会议室:({}),设备:({})的开机功能设置多条.自动省略一条'.format(self.meeting_room_name, self.name))
                else:
                    self.on_info['channel_guid'] = channel_guid
                    self.on_info['control_type'] = control_type
                    self.on_info['realize_status'] = realize_status
                    self.on_info['self_guid'] = self.guid
            elif function_name == 0:
                #关机
                if self.off_info.get('channel_guid') != None:
                    yield logClient.tornadoWarningLog('会议室:({}),设备:({})的关机功能设置多条.自动省略一条'.format(self.meeting_room_name, self.name))
                else:
                    self.off_info['channel_guid'] = channel_guid
                    self.off_info['control_type'] = control_type
                    self.off_info['realize_status'] = realize_status
                    self.off_info['self_guid'] = self.guid
            else:
                yield logClient.tornadoWarningLog('会议室:({}),设备:({})的开关功能设置错误'.format(self.meeting_room_name,self.name))

    # todo:获取设备在线通道
    @gen.coroutine
    def getSelfOnlineFunction(self):
        data = {
            'database': self.company_db,
            'fields': ['guid',
                       'device_guid_id',
                       'control_channel_guid_id',
                       ],
            'eq': {
                'is_delete': False,
                'device_guid_id': self.guid,
            },
        }
        msg = yield mysqlClient.tornadoSelectOnly('d_device_online_channel', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_device_online_channel'))
            return
        if msg['lenght'] == 0:
            yield logClient.tornadoWarningLog('会议室:({}),设备({})没有定义在线状态通道'.format(self.meeting_room_name, self.name))
            return
        self.online_channel_guid = msg['msg'][0]['control_channel_guid_id']

    @gen.coroutine
    def mysqlStorage(self, table,info,time=None):
        data = {
            'database': self.company_db,
            'msg': {
                'create_time': datetime.datetime.now(),
                'update_time': datetime.datetime.now(),
                'is_delete': False,
                'guid': uuid.uuid1().urn.split(':')[2],
                'device_guid_id': self.guid,
            }
        }
        data['msg'] = {**data['msg'],**info}
        if time:
            data['msg']['create_time'] = time
            data['msg']['update_time'] = time
        msg = yield mysqlClient.tornadoInsertOne(table, data)
        if msg['ret'] == '0':
            pass
        else:
            yield logClient.tornadoErrorLog('数据库插入错误:{}'.format(table))

