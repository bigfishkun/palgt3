# coding:utf-8

import requests
import json

class proxy(object):
    '''Set a proxy
    @proxy
    def func():
        pass
    '''
    def __init__(self, fn):
        self.fn = fn
        # pass

    def __call__(self, *args, **kwargs):
        try:
            from urllib.request import install_opener, build_opener, ProxyHandler, HTTPBasicAuthHandler
        except ImportError:
            #from urllib2 import install_opener, build_opener, ProxyHandler, HTTPBasicAuthHandler
            pass

        r = requests.get('http://127.0.0.1:8000/?types=0&count=20&country=国内')
        ip_ports = json.loads(r.text)
        print(ip_ports)

        import random
        randomIndex = random.randint(0, 19)
        ip = ip_ports[randomIndex][0]
        port = ip_ports[randomIndex][1]
        proxies = {
            'http': 'http://%s:%s' % (ip, port),
            'https': 'http://%s:%s' % (ip, port)
        }
        proxy_support = ProxyHandler(proxies)
        proxy_auth_handler = HTTPBasicAuthHandler()
        #proxy_auth_handler.add_password('realm', 'host', 'user', 'pwd')
        # proxy_auth_handler = None
        opener = build_opener(proxy_support, proxy_auth_handler)
        install_opener(opener)
        return self.fn(*args, **kwargs)

# import tushare as ts
#
# @proxy
# def get_k_data(stock_code,begin_date, end_date):
#     return ts.get_k_data(stock_code, autype=None, start=begin_date, end=end_date, retry_count=100, pause=10)
#
# print(get_k_data('600848', '2017-01-01', '2018-12-31'))
