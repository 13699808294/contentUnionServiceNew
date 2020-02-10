import datetime
import json
import aiohttp
import requests
from tornado import gen, httpclient
from tornado.httputil import url_concat
from tornado.simple_httpclient import HTTPTimeoutError

from utils.my_json import json_dumps

try:
    from setting.setting import DEBUG, LOG_SERVICE_HOST, LOG_TOPIC, LOG_DIR
except ImportError:
    DEBUG = True
try:
    from setting.setting import LOG_LEVEL
except ImportError:
    LOG_LEVEL = 'DEBUG'

class LogClient():
    def __init__(self,
                 host :str='127.0.0.1',
                 port :str='8001',
                 debug_dir :str = None,
                 debug_header :str = '',
                 info_dir :str = None,
                 info_header :str = '',
                 warning_dir :str = None,
                 warning_header :str = '',
                 error_dir :str = None,
                 error_header :str = ''
                 ):
        self.host = host
        self.port = port
        self.debug_dir = debug_dir
        self.info_dir = info_dir
        self.warning_dir = warning_dir
        self.error_dir = error_dir
        self.debug_header = debug_header
        self.info_header = info_header
        self.warning_header = warning_header
        self.error_header = error_header
        self.level_name = ['DEBUG','INFO','WARNING','ERROR']
        self.level = 0

    @gen.coroutine
    def asyncTornadoRequest(self,
            url: str,
            params: dict = None,
            body: dict = None,
            method: str = 'GET',
            headers: dict = None,
            allow_nonstandard_methods: bool = False):
        if body:
            body = json_dumps(body)
        if params:
            url = url_concat(url, params)
        request = httpclient.HTTPRequest(
            url=url,
            headers=headers,
            method=method,
            body=body,
            allow_nonstandard_methods=allow_nonstandard_methods,
        )
        http = httpclient.AsyncHTTPClient()
        # i = 0
        # while True:
        try:
            resp = yield http.fetch(request)
            # break
        except Exception as e:
            # i += 1
            # if i >= 10:
            result = {
                'ret': 1,
                'msg': str(e),
            }
            return result
        result = json.loads(resp.body.decode())
        return result

    def updateDebugLogDir(self,dir):
        if dir[-1] == '/':
            dir = dir[:-1]
        self.debug_dir = dir

    def updateInfoLogDir(self,dir):
        if dir[-1] == '/':
            dir = dir[:-1]
        self.info_dir = dir

    def updateWarningLogDir(self,dir):
        if dir[-1] == '/':
            dir = dir[:-1]
        self.warning_dir = dir

    def updateErrorLogDir(self,dir):
        if dir[-1] == '/':
            dir = dir[:-1]
        self.error_dir = dir

    def updateDebugLogHeader(self,header):
        self.debug_header = header

    def updateInfoLogHeader(self,header):
        self.info_header = header

    def updateWarningLogHeader(self,header):
        self.warning_header = header

    def updateErrorLogHeader(self,header):
        self.error_header = header

    def setLogLevel(self,level_name):
        if level_name not in self.level_name:
            raise IndexError('日志等级不存在')
        if level_name == 'DEBUG':
            self.level = 0
        elif level_name == 'INFO':
            self.level = 1
        elif level_name == 'WARNING':
            self.level = 2
        elif level_name == 'ERROR':
            self.level = 3

    def debugLog(self,msg,company=None):
        if self.level > 0:
            return
        if DEBUG:
            print('\033[1;37;0m[{}] [DEBUG]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;47;0m {} \033[0m'.format(str(msg)))
            return
        if not self.debug_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.debug_dir
        data['log_msg'] = self.debug_header + msg
        if company != None: data['company'] = company
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.get('http://{}:{}/debug_log'.format(self.host,self.port), params=data,headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': resp.text(encoding='utf8'),
            }
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    def infoLog(self,msg,company=None):
        if self.level > 1:
            return
        if DEBUG:
            print('\033[1;36;0m[{}] [INFO]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;46;0m {} \033[0m'.format(str(msg)))
            return
        if not self.info_dir:
            raise Exception('没有定义info日志路径')
        data = {}
        data['log_dir'] = self.info_dir
        data['log_msg'] = self.info_header + msg
        if company != None: data['company'] = company
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.get('http://{}:{}/info_log'.format(self.host, self.port), params=data, headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': resp.text(encoding='utf8'),
            }
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    def warningLog(self,msg,company=None):
        if self.level > 2:
            return
        if DEBUG:
            print('\033[1;35;0m[{}] [WARNING]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;45;0m {} \033[0m'.format(str(msg)))
            return
        if not self.warning_dir:
            raise Exception('没有定义warning日志路径')
        data = {}
        data['log_dir'] = self.warning_dir
        data['log_msg'] = self.warning_header + msg
        if company != None: data['company'] = company
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.get('http://{}:{}/warning_log'.format(self.host, self.port), params=data, headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': resp.text(encoding='utf8'),
            }
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    def errorLog(self,msg,company=None):
        if self.level > 3:
            return
        if DEBUG:
            print('\033[1;34;0m[{}] [ERROR]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;41;0m {} \033[0m'.format(str(msg)))
            return
        if not self.error_dir:
            raise Exception('没有定义error日志路径')
        data = {}
        data['log_dir'] = self.error_dir
        data['log_msg'] = self.error_header + msg
        if company != None: data['company'] = company
        headers = {
            'content-type': 'application/json',
        }
        resp = requests.get('http://{}:{}/error_log'.format(self.host, self.port), params=data, headers=headers)
        try:
            result = resp.json()
        except:
            result = {
                'ret': 1,
                'msg': resp.text(encoding='utf8'),
            }
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    async def asyncioDebugLog(self,msg,company=None):
        if self.level > 0:
            return
        if DEBUG:
            print('\033[1;37;0m[{}] [DEBUG]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;47;0m {} \033[0m'.format(str(msg)))
            return
        if not self.debug_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.debug_dir
        data['log_msg'] = self.debug_header + msg
        if company != None: data['company'] = company

        url = 'http://{}:{}/debug_log'.format(self.host,self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url,params=data, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': await resp.text(encoding='utf8'),
                    }
                if result['ret'] != 0:
                    # raise Exception(result['msg'])
                    print((result['msg']),msg)

    async def asyncioInfoLog(self,msg,company=None):
        if self.level > 1:
            return
        if DEBUG:
            print('\033[1;36;0m[{}] [INFO]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;46;0m {} \033[0m'.format(str(msg)))
            return
        if not self.info_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.info_dir
        data['log_msg'] = self.info_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/info_log'.format(self.host,self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=data, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': await resp.text(encoding='utf8'),
                    }
                if result['ret'] != 0:
                    # raise Exception(result['msg'])
                    print((result['msg']),msg)

    async def asyncioWarningLog(self,msg,company=None):
        if self.level > 2:
            return
        if DEBUG:
            print('\033[1;35;0m[{}] [WARNING]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;45;0m {} \033[0m'.format(str(msg)))
            return
        if not self.warning_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.warning_dir
        data['log_msg'] = self.warning_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/warning_log'.format(self.host, self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=data, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': await resp.text(encoding='utf8'),
                    }
                if result['ret'] != 0:
                    # raise Exception(result['msg'])
                    print((result['msg']),msg)

    async def asyncioErrorLog(self,msg,company=None):
        if self.level > 3:
            return
        if DEBUG:
            print('\033[1;34;0m[{}] [ERROR]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;41;0m {} \033[0m'.format(str(msg)))
            return
        if not self.error_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.error_dir
        data['log_msg'] = self.error_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/error_log'.format(self.host, self.port)
        headers = {
            'content-type': 'application/json',
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=data, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                else:
                    result = {
                        'ret': 1,
                        'msg': await resp.text(encoding='utf8'),
                    }
                if result['ret'] != 0:
                    # raise Exception(result['msg'])
                    print((result['msg']),msg)

    @gen.coroutine
    def tornadoDebugLog(self, msg,company=None):
        if self.level > 0:
            return
        if DEBUG:
            print('\033[1;37;0m[{}] [DEBUG]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;47;0m {} \033[0m'.format(str(msg)))
            return
        if not self.debug_dir:
            raise Exception('没有定义debug日志路径')
        data = {}
        data['log_dir'] = self.debug_dir
        data['log_msg'] = self.debug_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/debug_log'.format(self.host, self.port)
        method = 'GET'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url=url,params=data, method=method, headers=headers)
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    @gen.coroutine
    def tornadoInfoLog(self, msg,company=None):
        if self.level > 1:
            return
        if DEBUG:
            print('\033[1;36;0m[{}] [INFO]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;46;0m {} \033[0m'.format(str(msg)))
            return
        if not self.info_dir:
            raise Exception('没有定义info日志路径')
        data = {}
        data['log_dir'] = self.info_dir
        data['log_msg'] = self.info_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/info_log'.format(self.host, self.port)
        method = 'GET'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url=url,params=data, method=method, headers=headers)
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)
    @gen.coroutine
    def tornadoWarningLog(self, msg,company=None):
        if self.level > 2:
            return
        if DEBUG:
            print('\033[1;35;0m[{}] [WARNING]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;45;0m {} \033[0m'.format(str(msg)))
            return
        if not self.warning_dir:
            raise Exception('没有定义warning日志路径')
        data = {}
        data['log_dir'] = self.warning_dir
        data['log_msg'] = self.warning_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/warning_log'.format(self.host, self.port)
        method = 'GET'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url=url,params=data, method=method, headers=headers)
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

    @gen.coroutine
    def tornadoErrorLog(self, msg,company=None):
        if self.level > 3:
            return
        if DEBUG:
            print('\033[1;34;0m[{}] [ERROR]:  \033[0m'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))+'\033[1;41;0m {} \033[0m'.format(str(msg)))
            return
        if not self.error_dir:
            raise Exception('没有定义error日志路径')
        data = {}
        data['log_dir'] = self.error_dir
        data['log_msg'] = self.error_header + msg
        if company != None: data['company'] = company
        url = 'http://{}:{}/error_log'.format(self.host, self.port)
        method = 'GET'
        headers = {
            'content-type': 'application/json',
        }
        result = yield self.asyncTornadoRequest(url=url,params=data, method=method, headers=headers)
        if result['ret'] != 0:
            # raise Exception(result['msg'])
            print((result['msg']),msg)

# 显示方式: 0（默认值）、1（高亮）、22（非粗体）、4（下划线）、24（非下划线）、 5（闪烁）、25（非闪烁）、7（反显）、27（非反显）
# 前景色: 30（黑色）、31（红色）、32（绿色）、 33（黄色）、34（蓝色）、35（洋 红）、36（青色）、37（白色）
# 背景色: 40（黑色）、41（红色）、42（绿色）、 43（黄色）、44（蓝色）、45（洋 红）、46（青色）、47（白色）
logClient = LogClient(host=LOG_SERVICE_HOST)
logClient.setLogLevel(LOG_LEVEL)
logClient.updateDebugLogDir(LOG_DIR)
logClient.updateDebugLogHeader('[{}]  '.format(LOG_TOPIC))
logClient.updateInfoLogDir(LOG_DIR)
logClient.updateInfoLogHeader('[{}]  '.format(LOG_TOPIC))
logClient.updateWarningLogDir(LOG_DIR)
logClient.updateWarningLogHeader('[{}]  '.format(LOG_TOPIC))
logClient.updateErrorLogDir(LOG_DIR)
logClient.updateErrorLogHeader('[{}]  '.format(LOG_TOPIC))


