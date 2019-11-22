PyApollo - Python Client for Ctrip's Apollo
================

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

方便Python接入配置中心框架 [Apollo](https://github.com/ctripcorp/apollo) 所开发的Python版本客户端。
Tested with python 3

基于https://github.com/filamoon/pyapollo修改

Installation
------------

``` shell
pip install apollo-client
```

# Features
* 实时同步配置
* 灰度配置
* 客户端容灾

# Usage

- 启动客户端长连接监听

``` python
client = ApolloClient(app_id=<appId>, cluster=<clusterName>, config_server_url=<configServerUrl>)
client.start()
```

- 获取Apollo的配置
  ```
  client.get_value(Key, DefaultValue)
  ```

# Contribution
  * Source Code: https://github.com/BruceWW/pyapollo
  * Issue Tracker: https://github.com/BruceWW/pyapollo/issues
  
# License
The project is licensed under the [Apache 2 license](https://github.com/zouyx/agollo/blob/master/LICENSE).

# Reference
Apollo : https://github.com/ctripcorp/apollo
