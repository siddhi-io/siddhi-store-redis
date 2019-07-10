/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.store.redis.utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * RedisInstance implementation for Single node mode
 **/
public class RedisSingleNodeInstance implements RedisInstance {
    private Jedis jedis;
    private boolean initialTraverse = true;
    private String key;

    public RedisSingleNodeInstance(Jedis jedis) {
        this.jedis = jedis;
    }

    public void hmset(String key, Map<String, String> values) {
        jedis.hmset(key, values);
    }

    public void hset(String key, String field, String value) {
        jedis.hset(key, field, value);
    }

    public String hget(String key, String field) {
        return jedis.hget(key, field);
    }

    public void sadd(String key, String... member) {
        jedis.sadd(key, member);
    }

    public void srem(String key, String... member) {
        jedis.srem(key, member);
    }

    public void del(String key) {
        jedis.del(key);
    }

    public ScanResult<String> sscan(String key, String cursor, ScanParams scanParams) {
        return jedis.sscan(key, cursor, scanParams);
    }

    public ScanResult<String> sscan(String key, String cursor) {
        return jedis.sscan(key, cursor);
    }

    public Map<String, String> hgetAll(String key) {
        return jedis.hgetAll(key);
    }

    public List<String> scan(List<HostAndPort> nodes, ScanParams scanParams) {
        if (initialTraverse) {
            initialTraverse = false;
            key = "0";
        }
        ScanResult<String> scanResult;
        List<String> resultList = new ArrayList<>();
        scanResult = jedis.scan(key, scanParams);
        resultList.addAll(scanResult.getResult());
        key = scanResult.getStringCursor();
        return resultList;
    }

    public String type(String key) {
        return jedis.type(key);
    }
    
    public Long expire(String key, int ttl) {
      return jedis.expire(key, ttl);
    }
}
