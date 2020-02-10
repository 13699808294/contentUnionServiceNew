import json
from tornado import web, gen
from utils.my_json import json_dumps

class BaseHanderView(web.RequestHandler):
    def set_default_headers(self) -> None:
        self.set_header('Content-Type' ,'application/json;charset=UTF-8')

    def write_error(self, status_code: int, **kwargs) -> None:
        self.write(u"<h1>出错了</h1>")
        self.write(u'<p>{}</p>'.format(kwargs.get('error_title' ,'')))
        self.write(u'<p>{}</p>'.format(kwargs.get('error_message' ,'')))

    def initialize(self ,**kwargs) -> None:
        self.mosquittoObject = kwargs.get('server')

    def prepare(self):
        if self.request.headers.get('Content-Type' ,'').startswith('application/json'):
            # try:
            if self.request.body:
                self.json_dict = json.loads(self.request.body)
            # except:
            #     self.json_dict = None
        else:
            self.json_dict = None

    def on_finish(self) -> None:
        pass






class DataView(BaseHanderView):

    @gen.coroutine
    def post(self):
        '''
            {
                meeting_room_guid:
                channel_guid_list:[]
            }
        '''
        # todo：获取参数
        try:
            data = json.loads(self.request.body)
        except:
            result = {'ret': 1}
            return self.write(json_dumps(result))
        meeting_room_guid = data.get('meeting_room_guid')
        channel_guid_list = data.get('channel_guid_list')
        meetingRoomObject = self.mosquittoObject.GuidToMeetingRoom.get(meeting_room_guid)
        if meetingRoomObject == None:
            result = {'ret': 1}
        else:
            info = {}
            for channel_guid in channel_guid_list:
                channel_object = meetingRoomObject.channel_dict.get(channel_guid)
                if channel_object == None:
                    info[channel_guid] = ''
                else:
                    info[channel_guid] = channel_object.getSelfStatus()
            else:
                result = {'ret': 0, 'msg': info}
        self.write(json_dumps(result))