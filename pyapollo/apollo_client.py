# -*- coding: utf-8 -*-
import json
import logging
import sys
import threading
import time

import requests


class ApolloClient(object):
    def __init__(self, app_id, cluster='default', config_server_url='http://localhost:8080', timeout=35):
        self.config_server_url = config_server_url
        self.appId = app_id
        self.cluster = cluster
        self.timeout = timeout
        self.stopped = False

        self._stopping = False
        self._logger = logging.getLogger(__name__)
        self._cache = {}
        self._notification_map = {'application': -1}

    def get_value(self, key, default_val, namespace='application', auto_fetch_on_cache_miss=False):
        if namespace not in self._cache:
            self._cache[namespace] = {}
            self._logger.info("Add namespace '%s' to local cache", namespace)

        if namespace not in self._notification_map:
            self._notification_map[namespace] = -1
            self._logger.info("Add namespace '%s' to local notification map", namespace)

        if key in self._cache[namespace]:
            return self._cache[namespace][key]
        else:
            if auto_fetch_on_cache_miss:
                return self._cached_http_get(key, default_val, namespace)
            else:
                return default_val

    def _cached_http_get(self, key, default_val, namespace='application'):
        url = '{}/configfiles/json/{}/{}/{}'.format(self.config_server_url, self.appId, self.cluster, namespace)
        r = requests.get(url)
        if r.ok:
            data = r.json()
            self._cache[namespace] = data
            self._logger.info('Updated local cache for namespace %s', namespace)
        else:
            data = self._cache[namespace]

        if key in data:
            return data[key]
        else:
            return default_val

    def _uncached_http_get(self, namespace='application'):
        url = '{}/configs/{}/{}/{}'.format(self.config_server_url, self.appId, self.cluster, namespace)
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            self._cache[namespace] = data['configurations']
            self._logger.info('Updated local cache for namespace %s release key %s: %s', namespace, data['releaseKey'],
                              repr(self._cache[namespace]))

    def start(self):
        t = threading.Thread(target=self._listener)
        t.start()

    def stop(self):
        self._stopping = True
        self._logger.info("Stopping listener...")

    def _listener(self):
        url = '{}/notifications/v2'.format(self.config_server_url)
        self._logger.info('Entering listener loop...')
        while not self._stopping:
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

            self._logger.info('Long polling returns %d: url=%s', r.status_code, r.request.url)

            if r.status_code == 304:
                # no change, loop
                self._logger.debug('No change, loop...')
                continue

            if r.status_code == 200:
                data = r.json()
                for entry in data:
                    ns = entry['namespaceName']
                    nid = entry['notificationId']
                    self._logger.info("%s has changes: notificationId=%d", ns, nid)
                    self._uncached_http_get(ns)
                    self._notification_map[ns] = nid
            else:
                self._logger.warn('Sleep...')
                time.sleep(self.timeout)

        self._logger.info("Listener stopped!")
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
