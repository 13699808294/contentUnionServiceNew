import asyncio
import os
import tornado
import uvloop
from tornado import httpserver
from tornado import web
from tornado.options import options
from tornado.platform.asyncio import BaseAsyncIOLoop

from apps.views import DataView
from setting.setting import DEBUG
from utils.baseAsync import BaseAsync

tornado.options.define('http_service_port', type=int, default=8021, help='服务器端口号')


class HttpService(BaseAsync):
    def __init__(self,mosquittoClient=None):
        self.mosquittoClient = mosquittoClient
        self.urlpatterns = [
            (r'/data', DataView, {'server': self.mosquittoClient}),
        ]

        app = web.Application(self.urlpatterns,
                              debug=DEBUG,
                              # autoreload=True,
                              # compiled_template_cache=False,
                              # static_hash_cache=False,
                              # serve_traceback=True,
                              static_path = os.path.join(os.path.dirname(__file__),'static'),
                              template_path = os.path.join(os.path.dirname(__file__),'template'),
                              autoescape=None,  # 全局关闭模板转义功能
                                      )
        web_service = httpserver.HTTPServer(app)
        web_service.listen(options.http_service_port)