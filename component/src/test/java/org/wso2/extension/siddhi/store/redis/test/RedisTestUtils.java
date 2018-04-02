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

package org.wso2.extension.siddhi.store.redis.test;


import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Objects;

public class RedisTestUtils {
    private static final String HOST = "localhost";
    private static JedisPool jedisPool;
    private static Jedis jedis;
    private static final String PASSWORD = "root";

    private static void createConnectionPool() throws ConnectionUnavailableException {
        jedisPool = new JedisPool(new JedisPoolConfig(), HOST);
        try {
            jedis = jedisPool.getResource();
            jedis.auth(PASSWORD);
        } catch (Exception e) {
            throw new ConnectionUnavailableException("Error while initializing the Redis connection to host : "
                    + HOST + " : " + e.getMessage(), e);
        }
    }

    public static int getRowsFromTable(String tableName) throws ConnectionUnavailableException {
        createConnectionPool();
        int rowCount;
        ScanParams scanParams = new ScanParams();
        scanParams.match(tableName + ":*");
        scanParams.count(100);
        ScanResult result = jedis.scan("0", scanParams);
        rowCount = result.getResult().size();
        return rowCount;
    }

    public static void cleanRedisDatabase() throws ConnectionUnavailableException {
        if (Objects.isNull(jedis)) {
            createConnectionPool();
        }
        jedis.flushAll();
    }
}
