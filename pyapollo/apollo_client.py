# -*- coding: utf-8 -*-
import json
import socket
import sys
import threading
import time

import requests


def get_local_ip():
    """
    获取本地IP地址
    :return:
    """
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 53))
        return s.getsockname()[0]
    except Exception as e:
        raise e
    finally:
        if s:
            s.close()


class ApolloClient(object):
    """
    apollo python client
    使用说明: http://wiki.intra.yongqianbao.com/pages/viewpage.action?pageId=11928499
    author: sunguangran@daixiaomi.com
    """

    def __init__(self, app_id, config_server, cluster='default', timeout=90, local_ip=None):
        self.config_server = config_server
        self._notification_map = {'application': -1}

        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.stopped = False
        self._stopping = False
        self.ip = local_ip if local_ip else get_local_ip()

        # 本地缓存
        self._cache = {}

    def _listener(self):
        while not self._stopping:
            self._long_poll()

        self.stopped = True

    # start the long polling loop
    def start(self):
        if len(self._cache) == 0:
            self._long_poll()

        t1 = threading.Thread(target=self._listener, args=())
        t1.setDaemon(True)
        t1.start()

        return self

    def stop(self):
        self._stopping = True

    def _http_get_cached(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server, self.appId, self.cluster, namespace, self.ip)
        resp = requests.get(url)
        if resp.ok:
            data = resp.json()
            self._cache[namespace] = data
        else:
            data = self._cache[namespace]

        return data[key] if key in data else default_val

    def _http_get_ignore_cache(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server, self.appId, self.cluster, namespace, self.ip)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']

    def _long_poll(self):
        url = '{}/notifications/v2'.format(self.config_server)
        notifications = []
        for key in self._notification_map:
            notification_id = self._notification_map[key]
            notifications.append({
                'namespaceName' : key,
                'notificationId': notification_id
            })

        resp = requests.get(url=url, params={
            'appId'        : self.appId,
            'cluster'      : self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }, timeout=self.timeout)

        if resp.status_code == 304:
            return

        if resp.status_code == 200:
            data = resp.json()
            for entry in data:
                ns = entry['namespaceName']
                nid = entry['notificationId']
                self._http_get_ignore_cache(ns)
                self._notification_map[ns] = nid
        else:
            time.sleep(self.timeout)

    def get_value(self, key, default_val=None, namespace='application', auto_fetch=False):
        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            self._long_poll()

        if namespace not in self._cache:
            self._cache[namespace] = {}
            if auto_fetch:
                return self._http_get_cached(key, default_val, namespace)

        if key in self._cache[namespace]:
            return self._cache[namespace][key]

        return default_val


if __name__ == '__main__':
    client = ApolloClient(app_id=1001, config_server='http://192.168.1.255:11111', cluster='default', timeout=90).start()
    if sys.version_info[0] < 3:
        v = raw_input('Press any key to quit...')
    else:
        v = input('Press any key to quit...')

    print(client.get_value(key='timeout', default_val='none', namespace='mue123'))

    client.stop()
