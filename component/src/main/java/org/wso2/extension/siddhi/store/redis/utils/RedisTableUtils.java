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

import org.wso2.extension.siddhi.store.redis.BasicCompareOperation;
import org.wso2.extension.siddhi.store.redis.RedisCompliedCondition;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;

/**
 * This class contains the utils methods for Redis tables
 **/
public class RedisTableUtils {
    private RedisTableUtils() {
        //preventing initialization
    }

    private static ThreadLocal<SecureRandom> secureRandom = ThreadLocal.withInitial(SecureRandom::new);

    //this method will be used to generate an id to add when there is no primary key is defined.
    public static String generateRecordID() {
        byte[] data = new byte[16];
        secureRandom.get().nextBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    public static BasicCompareOperation resolveCondition(RedisCompliedCondition compliedCondition, Map<String, Object>
            findConditionParameterMap) {
        BasicCompareOperation condition = compliedCondition.getCompiledQuery();
        for (Map.Entry<String, Object> entry : findConditionParameterMap.entrySet()) {
            Object value = entry.getValue();
            (condition.getStreamVariable()).setName(value);
        }
        return condition;
    }
}
