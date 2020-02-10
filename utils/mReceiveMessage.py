from tornado import gen


class MReceiveMessage():
    def __init__(self,client, userdata, message=None,topic=None,data=None):
        self.mosquittoConnectObject = client
        self.mosquittoUserData = userdata

        self.header = None
        self.guid = None
        self.direction = None
        self.controlbus = None
        self.event = None
        self.type = None
        self.port = None
        self.channel = None
        if message != None:
            self.topic = message.topic
            self.data = message.payload.decode()
        else:
            if topic != None:
                self.topic = topic
            if data != None:
                self.data = data
        self.topicList = self.topicList()

    #/aaiot/101/send/controlbus/system/heartbeat
    def topicList(self):
        topic_list = self.topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        self.header = topic_list[0]
        self.guid = topic_list[1]
        self.direction = topic_list[2]
        self.controlbus = topic_list[3]
        self.event = topic_list[4]
        self.type = topic_list[5]
        try:
            self.port = topic_list[6]
            self.channel = topic_list[7]
        except:
            self.port = None
            self.channel = None
        return topic_list
