package org.wso2.extension.siddhi.store.redis.exceptions;

import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

/**
 * This is the exception class
 **/
public class RedisTableException extends SiddhiAppRuntimeException {
    public RedisTableException(String message) {
        super(message);
    }

    public RedisTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
