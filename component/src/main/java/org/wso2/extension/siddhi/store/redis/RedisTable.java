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

package org.wso2.extension.siddhi.store.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.redis.exceptions.RedisTableException;
import org.wso2.extension.siddhi.store.redis.utils.RedisTableConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.wso2.extension.siddhi.store.redis.utils.RedisTableUtils.generateRecordID;
import static org.wso2.extension.siddhi.store.redis.utils.RedisTableUtils.resolveCondition;

/**
 * This class contains the Event table implementation for Redis
 **/

@Extension(
        name = "redis",
        namespace = "store",
        description = "This extension assigns data source and connection instructions to event tables. It also " +
                "implements read write operations on connected datasource",
        parameters = {
                @Parameter(name = "host",
                        description = "host name of the redis server",
                        type = {DataType.STRING}),
                @Parameter(name = "port",
                        description = "port which redis server can be accessed. If this is not specified via the " +
                                "parameter,the default redis port '6379' will be used",
                        type = {DataType.LONG}, optional = true, defaultValue = "6379"),

                @Parameter(name = "table.name",
                        description = "The name with which the event table should be persisted in the store. If no" +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "The tale name defined in the siddhi app"),
                @Parameter(name = "password",
                        description = "password to connect to redis server",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "will try to connect without authenticate")
        },
        examples = {
                @Example(
                        syntax = "@store(type='redis',host='localhost',port=6379, password='root',table" +
                                ".name='fooTable')" +
                                "define table fooTable(time long, date String)",
                        description = "above collection will create a redis table with the name FooTable"
                )
        }
)

public class RedisTable extends AbstractRecordTable {
    private static final Log LOG = LogFactory.getLog(RedisTable.class);
    private List<Attribute> attributes;
    private List<String> primaryKeys = Collections.emptyList();
    private JedisPool jedisPool;
    private String host = "localhost";
    private char[] password;
    private int port = RedisTableConstants.DEFAULT_PORT;
    private String tableName;
    private Jedis jedis;
    private List<String> indices = Collections.emptyList();

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();

        // retrieve annotations
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        Annotation storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE, tableDefinition
                .getAnnotations());
        Annotation indexAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX, tableDefinition
                .getAnnotations());
        // set primaryKeys , indices and store parameters
        if (Objects.nonNull(primaryKeyAnnotation)) {
            this.primaryKeys = new ArrayList<>();
            primaryKeyAnnotation.getElements().forEach(element -> this.primaryKeys.add(element.getValue().trim()));
        }
        if (Objects.nonNull(indexAnnotation)) {
            this.indices = new ArrayList<>();
            List<Element> indexingElements = indexAnnotation.getElements();
            indexingElements.forEach(element -> this.indices.add(element.getValue().trim()));
        }

        host = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_HOST);

        if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT))) {
            port = Integer.parseInt(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT));
        }

        if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD))) {
            password = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD).toCharArray();
        }
        if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_TABLE_NAME))) {
            tableName = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_TABLE_NAME);
        } else {
            tableName = tableDefinition.getId();
        }


    }

    @Override
    protected void add(List<Object[]> records) {
        Map<String, String> attributeMap = new HashMap<>();
        try {
            records.forEach(record -> {
                StringBuilder keyGenBuilder = new StringBuilder(tableName);
                int i = 0;
                for (Attribute attribute : attributes) {
                    attributeMap.put(attribute.getName(), record[i++].toString());
                }
                if (Objects.nonNull(primaryKeys) && !primaryKeys.isEmpty()) {
                    createTablesWithPrimaryKey(attributeMap, keyGenBuilder);
                } else {
                    //generate a record id for each record if the primaryKey is not defined
                    String id = generateRecordID();
                    keyGenBuilder.append(":").append(id);
                    jedis.hmset(keyGenBuilder.toString(), attributeMap);
                    if (Objects.nonNull(indices) && !indices.isEmpty()) {
                        //create a set for each indexed column
                        createIndexTable(attributeMap, keyGenBuilder);
                    } else {
                        LOG.debug("There are no indexed columns defined in table " + tableName);
                    }
                }
            });
        } catch (JedisException e) {
            throw new RedisTableException("Error while inserting records to Redis Event Table : "
                    + tableName + ". ", e);
        }
    }


    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        try {
            BasicCompareOperation condition = resolveCondition((RedisCompliedCondition) compiledCondition,
                    findConditionParameterMap);
            return new RedisIterator(jedis, attributes, condition, tableName, primaryKeys, indices);
        } catch (JedisDataException e) {
            throw new RedisTableException("Error while searching the records in Redis Event Table : " + tableName, e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        RecordIterator iterator = find(containsConditionParameterMap, compiledCondition);
        return iterator.hasNext();
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        try {

            for (Map<String, Object> deleteConditionParameter : deleteConditionParameterMaps) {
                BasicCompareOperation condition = resolveCondition((RedisCompliedCondition) compiledCondition,
                        deleteConditionParameter);
                deleteFromTable(condition);
            }
        } catch (JedisDataException e) {
            throw new RedisTableException("Error while deleting records from table : " + tableName, e);
        }

    }

    /**
     *
     * **/
    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions,
                          List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {
        try {
            updateTable(updateCondition, updateSetParameterMaps, updateConditionParameterMaps, true);
        } catch (JedisDataException e) {
            throw new RedisTableException("Error while updating records from table : " + tableName, e);
        }
    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        try {
            updateTable(updateCondition, updateSetParameterMaps, updateConditionParameterMaps, false);
        } catch (JedisDataException e) {
            throw new RedisTableException("Error while updating records from table : " + tableName, e);
        }
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        RedisConditionVisitor visitor = new RedisConditionVisitor();
        expressionBuilder.build(visitor);
        return new RedisCompliedCondition(visitor.returnCondition());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        try {
            if (jedisPool == null) {
                jedisPool = new JedisPool(new JedisPoolConfig(), host, port);
                jedis = jedisPool.getResource();
                //if authentication is provided use it to connect to redis server
                if (Objects.nonNull(password)) {
                    jedis.auth(String.valueOf(password));
                }
            }
        } catch (JedisConnectionException e) {
            throw new ConnectionUnavailableException("Error while initializing the Redis event table: "
                    + tableName + " : "
                    + e
                    .getMessage
                            (), e);
        }

    }

    @Override
    protected void disconnect() {
        try {
            if (Objects.nonNull(jedisPool) && !jedisPool.isClosed()) {
                jedisPool.close();
            }
        } catch (JedisConnectionException e) {
            LOG.error("Error while closing the redis client for table: " + tableName + " : ", e);
        }

    }

    @Override
    protected void destroy() {
        try {
            if (Objects.nonNull(jedisPool)) {
                jedisPool.destroy();
            }
        } catch (JedisConnectionException e) {
            LOG.error("Error while closing the redis client for table: " + tableName + " : ", e);
        }
    }

    private void createIndexTable(Map<String, String> attributeMap, StringBuilder builder) {
        for (String index : indices) {
            jedis.sadd(tableName + ":" + index + ":" + attributeMap.get(index), builder.toString());
        }
    }

    private void createTablesWithPrimaryKey(Map<String, String> attributeMap, StringBuilder keyGenBuilder) {
        keyGenBuilder.append(":").append(attributeMap.get(primaryKeys.get(0)));
        //create hashes for each record
        jedis.hmset(keyGenBuilder.toString(), attributeMap);
        if (Objects.nonNull(indices) && !indices.isEmpty()) {
            //create a set for each indexed column
            createIndexTable(attributeMap, keyGenBuilder);
        } else {
            LOG.debug("There are no indexed columns defined in table " + tableName);
        }
    }

    private void deleteFromTable(BasicCompareOperation condition) {
        StoreVariable storeVariable = condition.getStoreVariable();
        StreamVariable streamVariable = condition.getStreamVariable();
        Map<String, String> recordMap;
        if (streamVariable.getName().equalsIgnoreCase("true")) {
            deleteAllRecords();
        } else {
            if (primaryKeys.contains(storeVariable.getName())) {

                recordMap = jedis.hgetAll(tableName + ":" + streamVariable.getName());
                for (String index : indices) {
                    jedis.srem(tableName + ":" + index + ":" + recordMap.get(index), tableName + ":" +
                            streamVariable.getName());
                }
                jedis.del(tableName + ":" + streamVariable.getName());

            } else if (indices.contains(storeVariable.getName())) {
                ScanResult indexedResults;

                indexedResults = jedis.sscan(tableName + ":" + storeVariable.getName() + ":" +
                        streamVariable.getName(), "0");

                for (Object recordID : indexedResults.getResult()) {
                    jedis.del(recordID.toString());
                }
                jedis.del(tableName + ":" + storeVariable.getName() + ":" + streamVariable.getName());
            } else {
                throw new OperationNotSupportedException("Cannot delete records by  '"
                        .concat(storeVariable.getName()).concat("' since the field is nether indexed nor primary key"));
            }
        }
    }

    private void deleteAllRecords() {
        ScanResult scanResult;
        List scanResultsList;
        ScanParams scanParams = new ScanParams();
        scanParams.match(tableName + ":*");
        scanParams.count(RedisTableConstants.REDIS_BATCH_SIZE);
        scanResult = jedis.scan("0", scanParams);
        scanResultsList = scanResult.getResult();
        if (scanResult.getStringCursor().equals("0")) {
            for (Object recodeID : scanResultsList) {
                jedis.del(recodeID.toString());
            }
        } else {
            while (Long.parseLong(scanResult.getStringCursor()) > 0) {
                scanResult = jedis.scan(scanResult.getStringCursor(), scanParams);
                scanResultsList.addAll(scanResult.getResult());
                if (!scanResultsList.isEmpty()) {
                    for (Object recodeID : scanResultsList) {
                        jedis.del(recodeID.toString());
                    }
                }
            }
        }
    }

    private void updateTable(CompiledCondition updateCondition, List<Map<String, Object>> updateSetParameterMaps,
                             List<Map<String, Object>> updateConditionParameterMaps, boolean updateOnly) {
        for (Map<String, Object> updateConditionParameter : updateConditionParameterMaps) {
            BasicCompareOperation condition = resolveCondition((RedisCompliedCondition) updateCondition,
                    updateConditionParameter);
            StoreVariable storeVariable = condition.getStoreVariable();
            StreamVariable streamVariable = condition.getStreamVariable();
            if (updateOnly && primaryKeys.contains(storeVariable.getName())) {
                Map<String, String> exisetRecord = jedis.hgetAll(tableName + ":" + streamVariable.getName());
                if (!exisetRecord.isEmpty()) {
                    updateOnPrimaryKey(updateSetParameterMaps, condition);
                } else {
                    LOG.warn("Record " + storeVariable.getName() + " = " + streamVariable.getName() +
                            " that trying to " +
                            "update does not exist in table : " + tableName + ". ");
                }
            } else if (!updateOnly && primaryKeys.contains(storeVariable.getName())) {
                updateOnPrimaryKey(updateSetParameterMaps, condition);
            } else if (updateOnly && indices.contains(storeVariable.getName())) {
                updateOrAddOnIndex(updateSetParameterMaps, condition);
            } else if (!updateOnly && indices.contains(storeVariable.getName())) {
                LOG.debug("Existing records will be updated where " + storeVariable.getName() + " = " +
                        streamVariable.getName() + ". ");
                updateOrAddOnIndex(updateSetParameterMaps, condition);
            }
        }
    }

    private void updateOnPrimaryKey(List<Map<String, Object>> updateSetParameterMaps, BasicCompareOperation condition) {
        StreamVariable streamVariable = condition.getStreamVariable();
        for (Map<String, Object> updateSetParameters : updateSetParameterMaps) {
            for (Map.Entry<String, Object> entry : updateSetParameters.entrySet()) {
                if (indices.contains(entry.getKey())) {
                    String existingValue = jedis.hget(tableName + ":" + streamVariable.getName(), entry.getKey());
                    jedis.srem(tableName + ":" + entry.getKey() + ":" + existingValue,
                            (tableName + ":" + streamVariable.getName()));
                    jedis.sadd(tableName + ":" + entry.getKey() + ":" + entry.getValue(),
                            tableName + ":" + streamVariable.getName());
                }
                String hashKey = tableName + ":" + streamVariable.getName();
                String key = entry.getKey();
                String val = entry.getValue().toString();
                jedis.hset
                        (hashKey, key, val);
            }
        }
    }

    private void updateOrAddOnIndex(List<Map<String, Object>> updateSetParameterMaps, BasicCompareOperation condition) {
        StoreVariable storeVariable = condition.getStoreVariable();
        StreamVariable streamVariable = condition.getStreamVariable();
        String redisKey = tableName + ":" + storeVariable.getName() + ":" + streamVariable.getName();
        ScanResult result = jedis.sscan(redisKey, "0");
        List indexedResults = result.getResult();
        if (Long.parseLong(result.getStringCursor()) > 0) {
            while (Long.parseLong(result.getStringCursor()) > 0) {
                result = jedis.sscan(redisKey, result.getStringCursor());
                indexedResults.addAll(result.getResult());
                executeUpdateOperation(updateSetParameterMaps, indexedResults);
            }
        } else {
            executeUpdateOperation(updateSetParameterMaps, indexedResults);
        }
    }

    private void executeUpdateOperation(List<Map<String, Object>> updateSetParameterMaps, List indexedResults) {
        for (Object indexedResult : indexedResults) {
            for (Map<String, Object> updateSetParameters : updateSetParameterMaps) {
                for (Map.Entry<String, Object> entry : updateSetParameters.entrySet()) {
                    //if the stream parameter value is null
                    if (entry.getValue() == null) {
                        entry.setValue("");
                    }
                    if (primaryKeys.contains(entry.getKey())) {
                        throw new OperationNotSupportedException("Primary Key cannot be update in table : "
                                + tableName);
                    } else {
                        executeUpdateOperationOnIndexedValues(entry, indexedResult);
                    }
                }
            }
        }
    }

    private void executeUpdateOperationOnIndexedValues(Map.Entry<String, Object> entry, Object indexedResult) {
        String oldValue = jedis.hget(indexedResult.toString(), entry.getKey());
        jedis.hset(indexedResult.toString(), entry.getKey(), entry.getValue().toString());
        jedis.sadd(tableName + ":" + entry.getKey() + ":" + entry.getValue(), indexedResult.toString());
        if (!oldValue.isEmpty()) {
            jedis.srem(tableName + ":" + entry.getKey() + oldValue, indexedResult.toString());
        }
    }
}
