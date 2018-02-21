package org.wso2.extension.siddhi.store.redis.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.redis.RedisCompliedCondition;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

/**
 * This class contains the utils methods for Redis tables
 **/
public class RedisTableUtils {
    private static Log log = LogFactory.getLog(RedisTableUtils.class);
    private static ThreadLocal<SecureRandom> secureRandom = ThreadLocal.withInitial(SecureRandom::new);

    public static String generateRecordID() {
        byte[] data = new byte[16];
        secureRandom.get().nextBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    public static List<String> resolveCondition(RedisCompliedCondition compliedCondition, Map<String, Object>
            findConditionPArameterMap) {
        List<String> condition = compliedCondition.getCompiledQuery();
        ListIterator<String> iterator;
        for (Map.Entry<String, Object> entry : findConditionPArameterMap.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            StringBuilder namePlaceholder = new StringBuilder("[" + name + "]");
            String stringValue;
            for (iterator = condition.listIterator(); iterator.hasNext(); ) {
                stringValue = iterator.next();
                if (stringValue.equals(namePlaceholder.toString())) {
                    iterator.set(value.toString());
                }
            }
        }
        return condition;
    }
}
