import asyncio
import datetime

import uvloop
from tornado import gen
from tornado.platform.asyncio import BaseAsyncIOLoop

from setting.setting import DATABASES, MY_SQL_SERVER_HOST
from utils.MysqlClient import MysqlClient
from utils.logClient import logClient


class Rule():
    def __init__(self,
                 guid,
                 meeting_room_guid,
                 rule_handle,
                 device_guid,
                 scene_guid,
                 execute_grade,
                 last_execute_time,
                 conditions
            ):
        self.guid = guid
        self.meeting_room_guid = meeting_room_guid

        self.rule_handle = rule_handle
        self.device_guid = device_guid
        self.scene_guid = scene_guid
        self.execute_grade = execute_grade

        self.executeCondition = []
        try:
            self.last_execute_time = datetime.datetime.strptime(last_execute_time, "%Y-%m-%d %H:%M:%S")
        except:
            self.last_execute_time = None
        self.UpdateSelfCondition(conditions)

    def UpdateSelfCondition(self,condition_list):
        '''
        时间触发:
        等于某时间规则:
        等于设定时间时,条件有效,过时则条件无效,   (有效时间1分钟,无效时间24小时/分钟-1分钟)
        不等于设定时间都有效,设定时间无效       (有效时间为24小时/分钟-1分钟,无效时间为1分钟)
        大于,大于等于,设定时间-00:00之间有效, (有效时间设定时间-00:00,无效时间00:00-设定时间)
        小于,小于等于,00:00-设定时间之间有效, (有效时间00:00-设定时间,无效时间设定时间-00:00)
        '''
        for condition_info in condition_list:
            trigger_type = condition_info.get('trigger_type')
            logic_index = condition_info.get('logic_index')

            info = {}
            info['status'] = False

            info['trigger_type'] = trigger_type
            info['compare_type'] = condition_info.get('compare_type')
            #时间触发
            if trigger_type == 0:
                info['rule_time'] = condition_info.get('rule_time')
                info['repetition_type'] = condition_info.get('repetition_type')
            #会议室状态触发
            elif trigger_type == 1:
                info['status_name_guid'] = condition_info.get('status_name_guid')
                info['keep_time'] = condition_info.get('keep_time')
                info['compare_value'] = condition_info.get('compare_value')
            #排程状态触发
            elif trigger_type == 2:
                info['schedule_status'] = condition_info.get('schedule_status')
                info['after_time'] = condition_info.get('after_time')
                info['before_time'] = condition_info.get('before_time')
            #识别触发
            elif trigger_type == 3:
                info['recognition_type'] = condition_info.get('recognition_type')
                info['device_sn'] = condition_info.get('device_sn')

            if logic_index < 100:
                continue
            index = logic_index//100
            while True:
                if len(self.executeCondition) < index:
                    self.executeCondition.append([])
                else:
                    break
            self.executeCondition[index-1].append(info)
            pass

        '''
        条件:
        self.executeCondition [
        [
            {
                'trigger_type':0,
                ...
            },
        ],
        ]
        '''


