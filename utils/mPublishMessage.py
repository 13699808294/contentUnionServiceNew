


class MPublishMessage():
    def __init__(self,topic, payload=None, qos=0, retain=False):
        self.topic = topic
        self.payload=payload
        self.qos=qos
        self.retain = retain
