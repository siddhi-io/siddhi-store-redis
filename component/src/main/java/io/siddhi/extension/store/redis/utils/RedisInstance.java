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
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;

/**
 * Redis Instance class
 **/
public interface RedisInstance {

    void hmset(String key, Map<String, String> values);

    void hset(String key, String field, String value);

    String hget(String key, String field);

    void sadd(String key, String... member);

    void srem(String key, String... member);

    void del(String key);

    ScanResult<String> sscan(String key, String cursor, ScanParams scanParams);

    ScanResult<String> sscan(String key, String cursor);

    Map<String, String> hgetAll(String key);

    List<String> scan(List<HostAndPort> nodes, ScanParams scanParams);

    String type(String key);
    
    public Long expire(String key, int ttl);
}
