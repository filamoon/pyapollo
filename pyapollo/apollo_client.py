# -*- coding: utf-8 -*-
import json
import logging
import sys
import threading
import time
import os

import requests

LOGGER = logging.getLogger(__name__)


class ApolloClient(object):
    def __init__(self,
                 app_id,
                 cluster='default',
                 config_server_url='http://localhost:8080',
                 timeout=35,
                 on_change=None,
                 ip=None,
                 conf_dir=None):
        self.config_server_url = config_server_url
        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.on_change_cb = on_change
        self.stopped = False
        self.init_ip(ip)

        # 初始化本地配置目录
        self.conf_dir = conf_dir or os.getcwd()
        if not os.path.exists(self.conf_dir):
            os.makedirs(self.conf_dir)

        self._stopping = False
        self._cache = {}
        self._notification_map = {'application': -1}

    def init_ip(self, ip):
        if ip:
            self.ip = ip
        else:
            import socket
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(('8.8.8.8', 53))
                ip = s.getsockname()[0]
            finally:
                s.close()
            self.ip = ip

    # Main method
    def get_value(self,
                  key,
                  default_val=None,
                  namespace='application',
                  auto_fetch_on_cache_miss=False):
        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            LOGGER.info("Add namespace '%s' to local notification map",
                        namespace)

        if namespace not in self._cache:
            self._cache[namespace] = {}
            LOGGER.info("Add namespace '%s' to local cache", namespace)
            # This is a new namespace, need to do a blocking fetch to populate the local cache
            self._long_poll()

        if key in self._cache[namespace]:
            return self._cache[namespace][key]
        else:
            if auto_fetch_on_cache_miss:
                return self._cached_http_get(key, default_val, namespace)
            else:
                return default_val

    def get_conf_file(self, namespace='app.yaml', auto_failover=True):
        """ 获取指定namespace的配置文件，非properities格式 """

        # 非properities格式的配置(yaml|json)，存储在content字段中
        value = self.get_value(
            'content',
            default_val=self._get_conf_from_disk(namespace)
            if auto_failover else None,
            namespace=namespace,
            auto_fetch_on_cache_miss=True)

        if value is None:
            return None

        if auto_failover:
            self._save_conf_to_disk(namespace, value)

        return self._loads(namespace, value)

    def _save_conf_to_disk(self, namespace, data):
        """ 本地磁盘容错 """
        try:
            with open('%s/%s' % (self.conf_dir, namespace), 'wb+') as f:
                f.write(data.encode('utf-8'))
        except Exception as e:
            LOGGER.error('save conf to disk fail: %s' % e)

    def _get_conf_from_disk(self, namespace):
        """ 从磁盘获取配置 """
        try:
            with open('%s/%s' % (self.conf_dir, namespace)) as f:
                return f.read()
        except Exception as e:
            LOGGER.error('get conf from disk fail: %s' % e)

    def _loads(self, namespace, value):
        """ 反序列化配置数据 """
        _, ext = os.path.splitext(namespace)
        if ext == '.yaml':
            import yaml
            return yaml.load(value)
        elif ext == '.json':
            import json
            return json.loads(value)
        else:
            # 其它格式，直接返回原始值
            return value

    # Start the long polling loop. Two modes are provided:
    # 1: thread mode (default), create a worker thread to do the loop. Call self.stop() to quit the loop
    # 2: eventlet mode (recommended), no need to call the .stop() since it is async
    def start(self,
              use_eventlet=False,
              eventlet_monkey_patch=False,
              catch_signals=True):
        # First do a blocking long poll to populate the local cache, otherwise we may get racing problems
        if len(self._cache) == 0:
            self._long_poll()
        if use_eventlet:
            import eventlet
            if eventlet_monkey_patch:
                eventlet.monkey_patch()
            eventlet.spawn(self._listener)
        else:
            if catch_signals:
                import signal
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                signal.signal(signal.SIGABRT, self._signal_handler)
            t = threading.Thread(target=self._listener)
            t.start()

    def stop(self):
        self._stopping = True
        LOGGER.info("Stopping listener...")

    def _cached_http_get(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(
            self.config_server_url, self.appId, self.cluster, namespace,
            self.ip)
        r = requests.get(url)
        if r.ok:
            data = r.json()
            self._cache[namespace] = data
            LOGGER.info('Updated local cache for namespace %s', namespace)
        else:
            data = self._cache[namespace]

        if key in data:
            return data[key]
        else:
            return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server_url,
                                                 self.appId, self.cluster,
                                                 namespace, self.ip)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']
            LOGGER.info(
                'Updated local cache for namespace %s release key %s: %s',
                namespace, data['releaseKey'], repr(self._cache[namespace]))

    def _signal_handler(self, signal, frame):
        LOGGER.info('You pressed Ctrl+C!')
        self._stopping = True

    def _long_poll(self):
        url = '{}/notifications/v2'.format(self.config_server_url)
        notifications = []
        for key in self._notification_map:
            notification_id = self._notification_map[key]
            notifications.append({
                'namespaceName': key,
                'notificationId': notification_id
            })

        r = requests.get(
            url=url,
            params={
                'appId': self.appId,
                'cluster': self.cluster,
                'notifications': json.dumps(notifications, ensure_ascii=False)
            },
            timeout=self.timeout)

        LOGGER.debug('Long polling returns %d: url=%s', r.status_code,
                     r.request.url)

        if r.status_code == 304:
            # no change, loop
            LOGGER.debug('No change, loop...')
            return

        if r.status_code == 200:
            data = r.json()
            for entry in data:
                ns = entry['namespaceName']
                nid = entry['notificationId']
                LOGGER.info("%s has changes: notificationId=%d", ns, nid)
                self._uncached_http_get(ns)
                self._notification_map[ns] = nid

                if self.on_change_cb is not None and self._cache.get(
                        ns) is not None:
                    self.on_change_cb(ns, self._cache.get(ns))
        else:
            LOGGER.warn('Sleep...')
            time.sleep(self.timeout)

    def _listener(self):
        LOGGER.info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()

        LOGGER.info("Listener stopped!")
        self.stopped = True


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    client = ApolloClient('pycrawler')
    client.start()
    if sys.version_info[0] < 3:
        v = raw_input('Press any key to quit...')
    else:
        v = input('Press any key to quit...')

    client.stop()
    while not client.stopped:
        pass
