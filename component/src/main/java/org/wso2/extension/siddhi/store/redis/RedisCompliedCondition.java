package org.wso2.extension.siddhi.store.redis;

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.List;

/**
 * CompiledCondition class
 **/
public class RedisCompliedCondition implements CompiledCondition {
    private List<String> compiledQuery;

    public RedisCompliedCondition(List<String> compiledQuery) {
        this.compiledQuery = compiledQuery;
    }

    public List<String> getCompiledQuery() {
        return compiledQuery;
    }

//    @Override
//    public List<String> toString() {
//
//        return getCompiledQuery();
//    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        return null;
    }
}
