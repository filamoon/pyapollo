# -*- coding: utf-8 -*-
import hashlib
import json
import logging
import os
import sys
import threading
import time

import requests


class ApolloClient(object):
    def __init__(self, app_id, cluster='default', config_server_url='http://localhost:8080', timeout=35, ip=None,
                 cycle_time=300, cache_file_path=None):
        self.config_server_url = config_server_url
        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.stopped = False
        self.init_ip(ip)

        self._stopping = False
        self._cache = {}
        self._notification_map = {'application': -1}
        self._cycle_time = cycle_time
        self._hash = dict()
        if cache_file_path is None:
            self._cache_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config')
        else:
            self._cache_file_path = cache_file_path
        self._path_checker()

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
        url = '{}/configfiles/json/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace,
                                                          self.ip)
        data = dict()
        try:
            r = requests.get(url)
            if r.ok:
                data = r.json()
                self._cache[namespace] = data
                logging.getLogger(__name__).info('Updated local cache for namespace %s', namespace)
                self._update_local_cache(data, namespace)
            else:
                if self._cache[namespace] is None or len(self._cache[namespace]) == 0:
                    logging.getLogger(__name__).info('cached http get configuration from local cache file')
                    data = self._get_local_cache(namespace)
                else:
                    data = self._cache[namespace]
        except BaseException as e:
            logging.getLogger(__name__).warning(str(e))
            data = self._get_local_cache(namespace)
        finally:
            if key in data:
                return data[key]
            else:
                return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}?ip={}'.format(self.config_server_url, self.appId, self.cluster, namespace, self.ip)
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                self._cache[namespace] = data['configurations']
                logging.getLogger(__name__).info('Updated local cache for namespace %s release key %s: %s',
                                             namespace, data['releaseKey'],
                                             repr(self._cache[namespace]))
                self._update_local_cache(data, namespace)
            else:
                data = self._get_local_cache(namespace)
                logging.getLogger(__name__).info('uncached http get configuration from local cache file')
                self._cache[namespace] = data['configurations']
        except BaseException as e:
            logging.getLogger(__name__).warning(str(e))
            data = self._get_local_cache(namespace)
            self._cache[namespace] = data['configurations']

    def _signal_handler(self, signal, frame):
        logging.getLogger(__name__).info('You pressed Ctrl+C!')
        self._stopping = True

    def _path_checker(self):
        """
        create configuration cache file directory if not exits
        :return:
        """
        if not os.path.isdir(self._cache_file_path):
            os.mkdir(self._cache_file_path)

    def _update_local_cache(self, data, namespace='application'):
        """
        if local cache file exits, update the content
        if local cache file not exits, create a version
        :param data: new configuration content
        :param namespace::s
        :return:
        """
        new_string = json.dumps(data)
        new_hash = hashlib.md5(new_string.encode('utf-8')).hexdigest()
        if self._hash[namespace] == new_hash:
            pass
        else:
            with open(os.path.join(self._cache_file_path, 'configuration_%s.txt' % namespace), 'w') as f:
                f.write(new_string)
            self._hash[namespace] = new_hash

    def _get_local_cache(self, namespace='application'):
        """
        get configuration from local cache file
        if local cache file not exits than return empty dict
        :param namespace:
        :return:
        """
        cache_file_path = os.path.join(self._cache_file_path, 'configuration_%s.txt' % namespace)
        if os.path.isfile(cache_file_path):
            with open(cache_file_path, 'r') as f:
                result = json.loads(f.readline())
            return result
        return dict()

    def _long_poll(self):
        url = '{}/notifications/v2'.format(self.config_server_url)
        notifications = []
        for key in self._notification_map:
            notification_id = self._notification_map[key]
            notifications.append({
                'namespaceName': key,
                'notificationId': notification_id
            })
        try:
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
                    return
            else:
                logging.getLogger(__name__).warning('Sleep...')
                time.sleep(self.timeout)
                return
        except requests.exceptions.ReadTimeout as e:
            logging.getLogger(__name__).warning(str(e))
        except requests.exceptions.ConnectionError as e:
            logging.getLogger(__name__).warning(str(e))
            self._load_local_cache_file()

    def _load_local_cache_file(self):
        """
        load all cached files from local path
        is only used while apollo server is unreachable
        :return:
        """
        for file in os.listdir(self._cache_file_path):
            file_path = os.path.join(self._cache_file_path, file)
            if os.path.isfile(file_path):
                namespace = file.split('.')[0].split('_')[1]
                with open(file_path) as f:
                    self._cache[namespace] = json.loads(f.read())['configurations']
        return True


    def _listener(self):
        logging.getLogger(__name__).info('Entering listener loop...')
        while not self._stopping:
            self._long_poll()
            time.sleep(self._cycle_time)

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
