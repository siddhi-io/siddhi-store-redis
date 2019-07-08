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

package org.wso2.extension.siddhi.store.redis.utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * RedisInstance implementation for Cluster mode
 **/
public class RedisClusterInstance implements RedisInstance {
    private JedisCluster jedisCluster;
    private boolean initialTraverse = true;
    private List<String> keys = new ArrayList();

    public RedisClusterInstance(JedisCluster jedis) {
        this.jedisCluster = jedis;
    }


    public void hmset(String key, Map<String, String> values) {
        jedisCluster.hmset(key, values);
    }

    public void hset(String key, String field, String value) {
        jedisCluster.hset(key, field, value);
    }

    public String hget(String key, String field) {
        return jedisCluster.hget(key, field);
    }

    public void sadd(String key, String... member) {
        jedisCluster.sadd(key, member);
    }

    public void srem(String key, String... member) {
        jedisCluster.srem(key, member);
    }

    public void del(String key) {
        jedisCluster.del(key);
    }

    public ScanResult<String> sscan(String key, String cursor, ScanParams scanParams) {
        return jedisCluster.sscan(key, cursor, scanParams);
    }

    public ScanResult<String> sscan(String key, String cursor) {
        return jedisCluster.sscan(key, cursor);
    }

    public Map<String, String> hgetAll(String key) {
        return jedisCluster.hgetAll(key);
    }

    public List<String> scan(List<HostAndPort> nodes, ScanParams scanParams) {
        int iterator = 0;
        if (initialTraverse) {
            initialTraverse = false;
            keys = new ArrayList<>(Collections.nCopies(nodes.size(), "0"));
        }
        ScanResult<String> scanResult;
        List<String> resultList = new ArrayList<>();
        for (HostAndPort node : nodes) {
            try (Jedis jedisNode = new Jedis(node.getHost(), node.getPort())) {
                scanResult = jedisNode.scan(keys.get(iterator), scanParams);
                resultList.addAll(scanResult.getResult());
                keys.set(iterator, scanResult.getStringCursor());
                iterator++;
            }
        }
        if (keys.stream().allMatch("0"::equals)) {
            keys.clear();
            initialTraverse = true;
        }
        return resultList;
    }

    public String type(String key) {
        return jedisCluster.type(key);
    }
    
    public Long expire(String key, int ttl) {
      return jedisCluster.expire(key, ttl);
    }
}
