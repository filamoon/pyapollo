# -*- coding: utf-8 -*-
import json
import logging
import sys
import threading
import time
import os

import requests


class ApolloClient(object):
    def __init__(self, app_id, cluster='default', config_server_url='http://localhost:8080', timeout=35, ip=None, conf_dir=None):
        self.config_server_url = config_server_url
        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.stopped = False
        self.init_ip(ip)
        self.conf_dir = conf_dir or os.getcwd()

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
    def get_value(self, key, default_val=None, namespace='application', auto_fetch_on_cache_miss=False):
        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            logging.getLogger(__name__).info("Add namespace '%s' to local notification map", namespace)

        if namespace not in self._cache:
            self._cache[namespace] = {}
            logging.getLogger(__name__).info("Add namespace '%s' to local cache", namespace)
            # This is a new namespace, need to do a blocking fetch to populate the local cache
            self._long_poll()

        if key in self._cache[namespace]:
            return self._cache[namespace][key]
        else:
            if auto_fetch_on_cache_miss:
                return self._cached_http_get(key, default_val, namespace)
            else:
                return default_val

    def _save_conf_to_disk(self, namespace, data):
        """ 本地磁盘容错 """
        try:
            with open('%s/%s' % (self.conf_dir, namespace), 'wb+') as f:
                f.write(data.encode('utf-8'))
        except Exception as e:
            logging.getLogger(__name__).error('save conf to disk fail: %s' % e)

    def _get_conf_from_disk(self, namespace):
        """ 从磁盘获取配置 """
        try:
            with open('%s/%s' % (self.conf_dir, namespace)) as f:
                return f.read()
        except Exception as e:
            logging.getLogger(__name__).error('get conf from disk fail: %s' % e)

    def _loads(self, namespace, conf_data):
        """ 反序列化配置数据 """
        _, ext = os.path.splitext(namespace)
        if ext == '.yaml':
            import yaml
            return yaml.load(conf_data) 
        elif ext == '.json':
            import json
            return json.loads(conf_data)
        else:
            raise Exception('unsupport configuration file extension')

    def get_conf_file(self, namespace='app.yaml', auto_failover=True):
        """ 获取带缓存的接口获取配置数据， 默认开启容错 auto_failover=True
            默认重试3次，每次sleep 1s
        """
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        _try_cnt = 0 
        _conf_data = None      
        while _try_cnt < 3:
            resp = requests.get(url)
            if resp.ok:
                body = resp.json()
                _conf_data = body.get('content', None) # 文件内容
                if auto_failover and _conf_data is not None:
                    self._save_conf_to_disk(namespace, _conf_data)
            else:
                time.sleep(1)
                _try_cnt += 1
                logging.getLogger(__name__).warning('get config file fail, status_code=%s, try again=%s' % (resp.status_code, _try_cnt))
                continue

        # 启用容错模式，尝试从本地加载配置
        if _conf_data is None and auto_failover:
            _conf_data = self._get_conf_from_disk(namespace)

        if _conf_data is None:
            raise Exception('get conf file fail, exit.')

        return self._loads(namespace, _conf_data)


    # Start the long polling loop. Two modes are provided:
    # 1: thread mode (default), create a worker thread to do the loop. Call self.stop() to quit the loop
    # 2: eventlet mode (recommended), no need to call the .stop() since it is async
    def start(self, use_eventlet=False, eventlet_monkey_patch=False, catch_signals=True):
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
        logging.getLogger(__name__).info("Stopping listener...")

    def _cached_http_get(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        r = requests.get(url)
        if r.ok:
            data = r.json()
            self._cache[namespace] = data
            logging.getLogger(__name__).info('Updated local cache for namespace %s', namespace)
        else:
            data = self._cache[namespace]

        if key in data:
            return data[key]
        else:
            return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']
            logging.getLogger(__name__).info('Updated local cache for namespace %s release key %s: %s',
                                             namespace, data['releaseKey'],
                                             repr(self._cache[namespace]))

    def _signal_handler(self, signal, frame):
        logging.getLogger(__name__).info('You pressed Ctrl+C!')
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

        r = requests.get(url=url, params={
            'appId': self.appId,
            'cluster': self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }, timeout=self.timeout)

        logging.getLogger(__name__).debug('Long polling returns %d: url=%s', r.status_code, r.request.url)

        if r.status_code == 304:
            # no change, loop
            logging.getLogger(__name__).debug('No change, loop...')
            return

        if r.status_code == 200:
            data = r.json()
            for entry in data:
                ns = entry['namespaceName']
                nid = entry['notificationId']
                logging.getLogger(__name__).info("%s has changes: notificationId=%d", ns, nid)
                self._uncached_http_get(ns)
                self._notification_map[ns] = nid
        else:
            logging.getLogger(__name__).warn('Sleep...')
            time.sleep(self.timeout)

    def _listener(self):
        logging.getLogger(__name__).info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()

        logging.getLogger(__name__).info("Listener stopped!")
        self.stopped = True


if __name__ == '__main__':
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
