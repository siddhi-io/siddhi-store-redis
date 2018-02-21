package org.wso2.extension.siddhi.store.redis.exceptions;

/**
 * Redis Iterator Exception
 * **/
public class RedisIteratorException extends RuntimeException {
    public RedisIteratorException() {

    }

    public RedisIteratorException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
