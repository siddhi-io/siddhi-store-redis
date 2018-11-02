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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.store.redis.beans.StoreVariable;
import org.wso2.extension.siddhi.store.redis.beans.StreamVariable;
import org.wso2.extension.siddhi.store.redis.exceptions.RedisTableException;
import org.wso2.extension.siddhi.store.redis.utils.RedisClusterInstance;
import org.wso2.extension.siddhi.store.redis.utils.RedisInstance;
import org.wso2.extension.siddhi.store.redis.utils.RedisSingleNodeInstance;
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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.wso2.extension.siddhi.store.redis.utils.RedisTableUtils.generateRecordID;
import static org.wso2.extension.siddhi.store.redis.utils.RedisTableUtils.resolveCondition;

/**
 * This class contains the Event table implementation for Redis
 **/

@Extension(
        name = "redis",
        namespace = "store",
        description = "This extension assigns data source and connection instructions to event tables. It also " +
                "implements read write operations on connected datasource. This extension only can be used to read " +
                "the data which persisted using the same extension since unique implementation has been used to map " +
                "the relational data in to redis's key and value representation",
        parameters = {
                @Parameter(name = "table.name",
                        description = "The name with which the event table should be persisted in the store. If no" +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "The tale name defined in the siddhi app"),
                @Parameter(name = "cluster.mode",
                        description = "This will decide the redis mode. if this is false, client will connect to a " +
                                "single redis node.",
                        type = {DataType.BOOL},
                        defaultValue = "false"),
                @Parameter(name = "nodes",
                        description = "host, port and the password of the node(s).In single node mode node details " +
                                "can be provided as follows- \"node='hosts:port@password'\" \nIn clustered mode host " +
                                "and port of all the master nodes should be provided separated by a comma(,). As an " +
                                "example \"nodes = 'localhost:30001,localhost:30002'\".",
                        type = {DataType.STRING}, optional = true,
                        defaultValue = "localhost:6379@root"),
        },
        examples = {
                @Example(
                        syntax = "@store(type='redis',nodes='localhost:6379@root',table.name='fooTable'," +
                                "cluster.mode=false)" +
                                "define table fooTable(time long, date String)",
                        description = "Above example will create a redis table with the name fooTable and work on a" +
                                "single redis node."
                ),
                @Example(
                        syntax = "@Store(type='redis', table.name='SweetProductionTable', " +
                                "nodes='localhost:30001,localhost:30002,localhost:30003', cluster.mode='true')\n" +
                                "@primaryKey('symbol')\n" +
                                "@index('price') \n" +
                                "define table SweetProductionTable (symbol string, price float, volume long);",
                        description = "Above example demonstrate how to use the redis extension to connect in to " +
                                "redis cluster. Please note that, as nodes all the master node's host and port should" +
                                " be provided in order to work correctly. In clustered node password will not be" +
                                "supported"
                )
        }
)

public class RedisTable extends AbstractRecordTable {
    private static final Logger log = LoggerFactory.getLogger(RedisTable.class);
    private List<Attribute> attributes;
    private List<String> primaryKeys = new ArrayList<>();
    private JedisPool jedisPool;
    private String host = RedisTableConstants.DEFAULT_HOST;
    private char[] password;
    private int port = RedisTableConstants.DEFAULT_PORT;
    private String tableName;
    private JedisCluster jedisCluster;
    private Jedis jedis;
    private List<String> indices = new ArrayList<>();
    private Boolean clusterModeEnabled = false;
    private List<HostAndPort> hostAndPortList = Arrays.asList(new HostAndPort(host, port));
    private RedisInstance redisInstance;

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
        if (primaryKeyAnnotation != null) {
            this.primaryKeys = new ArrayList<>();
            primaryKeyAnnotation.getElements().forEach(element -> this.primaryKeys.add(element.getValue().trim()));
        }
        if (indexAnnotation != null) {
            List<Element> indexingElements = indexAnnotation.getElements();
            indexingElements.forEach(element -> this.indices.add(element.getValue().trim()));
        }
        if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_CLUSTER_MODE) != null) {
            clusterModeEnabled = Boolean.parseBoolean(storeAnnotation.getElement(RedisTableConstants
                    .ANNOTATION_ELEMENT_CLUSTER_MODE));
        }
        if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_HOST) != null) {
            host = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_HOST);
        }
        if (clusterModeEnabled) {
            hostAndPortList = new ArrayList<>();
            if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_NODES) != null) {
                String[] nodes = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_NODES)
                        .split(",");
                for (String node : nodes) {
                    String[] clusterHostAndPort = node.split(":");
                    if (node.split(":").length == 2) {
                        hostAndPortList.add(new HostAndPort(clusterHostAndPort[0], Integer.parseInt
                                (clusterHostAndPort[1])));
                    }
                }
            }
        } else {
            if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_NODES) != null) {
                String node = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_NODES);
                String[] nodeDetails = node.split("@");
                if (nodeDetails.length > 0) {
                    String[] clusterHostAndPort = nodeDetails[0].split(":");
                    if (nodeDetails[0].split(":").length == 2) {
                        host = clusterHostAndPort[0];
                        port = Integer.parseInt(clusterHostAndPort[1]);
                    }
                    if (nodeDetails.length == 2) {
                        password = nodeDetails[1].toCharArray();
                    }
                }
            }
        }
        if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT) != null) {
            port = Integer.parseInt(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT));
        }
        if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD) != null) {
            password = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD).toCharArray();
        }
        if (storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_TABLE_NAME) != null) {
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
                if (primaryKeys != null && !primaryKeys.isEmpty()) {
                    if (isRecordExists(attributeMap)) {
                        throw new RedisTableException("Error While adding record to the table '" + tableName + "'. " +
                                "Record exists for primary key '" + primaryKeys.get(0) + "' with the value of " +
                                "'" + attributeMap.get(primaryKeys.get(0)) + "'. ");
                    }
                    createTablesWithPrimaryKey(attributeMap, keyGenBuilder);
                } else {
                    //generate a record id for each record if the primaryKey is not defined
                    String id = generateRecordID();
                    keyGenBuilder.append(":").append(id);
                    redisInstance.hmset(keyGenBuilder.toString(), attributeMap);
                    if (indices != null && !indices.isEmpty()) {
                        //create a set for each indexed column
                        createIndexTable(attributeMap, keyGenBuilder);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("There are no indexed columns defined in table " + tableName);
                        }
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
            return new RedisIterator(redisInstance, attributes, condition, tableName, primaryKeys, indices,
                    hostAndPortList);
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
                if (clusterModeEnabled) {
                    Set<HostAndPort> jedisClusterNodes = new HashSet<>(hostAndPortList);
                    jedisCluster = new JedisCluster(jedisClusterNodes);
                    redisInstance = new RedisClusterInstance(jedisCluster);
                } else {
                    jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, port);
                    jedis = jedisPool.getResource();
                    //if authentication is provided use it to connect to redis server
                    if (password != null) {
                        jedis.auth(String.valueOf(password));
                    }
                    redisInstance = new RedisSingleNodeInstance(jedis);
                }
            }
        } catch (JedisConnectionException e) {
            throw new ConnectionUnavailableException("Error while initializing the Redis event table: "
                    + tableName + " : " + e.getMessage(), e);
        }
    }


    @Override
    protected void disconnect() {
        try {
            if (clusterModeEnabled && jedisCluster != null) {
                jedisCluster.close();
            }
            if (jedisPool != null && !jedisPool.isClosed()) {
                jedisPool.close();
            }
        } catch (JedisConnectionException | IOException e) {
            log.error("Error while closing the redis client for table: " + tableName + " : ", e);
        }
    }

    @Override
    protected void destroy() {
        try {
            if (jedisPool != null) {
                jedisPool.destroy();
            }
        } catch (JedisConnectionException e) {
            log.error("Error while closing the redis client for table: " + tableName + " : ", e);
        }
    }

    private void createIndexTable(Map<String, String> attributeMap, StringBuilder builder) {
        for (String index : indices) {
            redisInstance.sadd(tableName + ":" + index + ":" + attributeMap.get(index), builder.toString());
        }
    }

    private void createTablesWithPrimaryKey(Map<String, String> attributeMap, StringBuilder keyGenBuilder) {
        keyGenBuilder.append(":").append(attributeMap.get(primaryKeys.get(0)));
        //create hashes for each record
        redisInstance.hmset(keyGenBuilder.toString(), attributeMap);
        if (indices != null && !indices.isEmpty()) {
            //create a set for each indexed column
            createIndexTable(attributeMap, keyGenBuilder);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("There are no indexed columns defined in table " + tableName);
            }
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
                recordMap = redisInstance.hgetAll(tableName + ":" + streamVariable.getName());
                for (String index : indices) {
                    redisInstance.srem(tableName + ":" + index + ":" + recordMap.get(index), tableName + ":" +
                            streamVariable.getName());
                }
                redisInstance.del(tableName + ":" + streamVariable.getName());
            } else if (indices.contains(storeVariable.getName())) {
                ScanResult indexedResults;
                indexedResults = redisInstance.sscan(tableName + ":" + storeVariable.getName() + ":" +
                        streamVariable.getName(), "0");
                for (Object recordID : indexedResults.getResult()) {
                    redisInstance.del(recordID.toString());
                }
                redisInstance.del(tableName + ":" + storeVariable.getName() + ":" + streamVariable.getName());
            } else {
                throw new OperationNotSupportedException("Cannot delete records by  '" + storeVariable.getName() +
                        "' since the field is nether indexed nor primary key");
            }
        }
    }

    private void deleteAllRecords() {
        List<String> scanResult;
        ScanParams scanParams = new ScanParams();
        scanParams.match(tableName + ":*");
        scanParams.count(RedisTableConstants.REDIS_BATCH_SIZE);
        scanResult = redisInstance.scan(hostAndPortList, scanParams);
        while (!scanResult.isEmpty()) {
            for (Object recodeID : scanResult) {
                redisInstance.del(recodeID.toString());
            }
            scanResult = redisInstance.scan(hostAndPortList, scanParams);
        }
    }

    private void updateTable(CompiledCondition
                                     updateCondition, List<Map<String, Object>> updateSetParameterMaps,
                             List<Map<String, Object>> updateConditionParameterMaps, boolean updateOnly) {
        for (Map<String, Object> updateConditionParameter : updateConditionParameterMaps) {
            BasicCompareOperation condition = resolveCondition((RedisCompliedCondition) updateCondition,
                    updateConditionParameter);
            StoreVariable storeVariable = condition.getStoreVariable();
            StreamVariable streamVariable = condition.getStreamVariable();
            if (updateOnly && primaryKeys.contains(storeVariable.getName())) {
                Map<String, String> exisetRecord = redisInstance.hgetAll(tableName + ":" + streamVariable.getName());
                if (!exisetRecord.isEmpty()) {
                    updateOnPrimaryKey(updateSetParameterMaps, condition);
                } else {
                    log.warn("Record " + storeVariable.getName() + " = " + streamVariable.getName() +
                            " that trying to " + "update does not exist in table : " + tableName + ". ");
                }
            } else if (!updateOnly && primaryKeys.contains(storeVariable.getName())) {
                updateOnPrimaryKey(updateSetParameterMaps, condition);
            } else if (updateOnly && indices.contains(storeVariable.getName())) {
                updateOrAddOnIndex(updateSetParameterMaps, condition);
            } else if (!updateOnly && indices.contains(storeVariable.getName())) {
                if (log.isDebugEnabled()) {
                    log.debug("Existing records will be updated where " + storeVariable.getName() + " = " +
                            streamVariable.getName() + ". ");
                }
                updateOrAddOnIndex(updateSetParameterMaps, condition);
            }
        }
    }

    private void updateOnPrimaryKey(List<Map<String, Object>> updateSetParameterMaps, BasicCompareOperation
            condition) {
        StreamVariable streamVariable = condition.getStreamVariable();
        for (Map<String, Object> updateSetParameters : updateSetParameterMaps) {
            for (Map.Entry<String, Object> entry : updateSetParameters.entrySet()) {
                if (indices.contains(entry.getKey())) {
                    String existingValue = redisInstance.hget(tableName + ":" + streamVariable.getName(),
                            entry.getKey());
                    redisInstance.srem(tableName + ":" + entry.getKey() + ":" + existingValue,
                            tableName + ":" + streamVariable.getName());
                    redisInstance.sadd(tableName + ":" + entry.getKey() + ":" + entry.getValue(),
                            tableName + ":" + streamVariable.getName());
                }
                String hashKey = tableName + ":" + streamVariable.getName();
                String key = entry.getKey();
                String val = entry.getValue().toString();
                redisInstance.hset(hashKey, key, val);
            }
        }
    }

    private void updateOrAddOnIndex(List<Map<String, Object>> updateSetParameterMaps, BasicCompareOperation
            condition) {
        StoreVariable storeVariable = condition.getStoreVariable();
        StreamVariable streamVariable = condition.getStreamVariable();
        String redisKey = tableName + ":" + storeVariable.getName() + ":" + streamVariable.getName();
        ScanResult result = redisInstance.sscan(redisKey, "0");
        List indexedResults = result.getResult();
        if (Long.parseLong(result.getStringCursor()) > 0) {
            while (Long.parseLong(result.getStringCursor()) > 0) {
                result = redisInstance.sscan(redisKey, result.getStringCursor());
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
        String oldValue = redisInstance.hget(indexedResult.toString(), entry.getKey());
        redisInstance.hset(indexedResult.toString(), entry.getKey(), entry.getValue().toString());
        redisInstance.sadd(tableName + ":" + entry.getKey() + ":" + entry.getValue(), indexedResult.toString());
        if (!oldValue.isEmpty()) {
            redisInstance.srem(tableName + ":" + entry.getKey() + oldValue, indexedResult.toString());
        }
    }

    private boolean isRecordExists(Map<String, String> attributeMap) {
        Map<String, String> resultMap = redisInstance.hgetAll(tableName + ":" + attributeMap.get(primaryKeys.get(0)));
        return !resultMap.isEmpty();
    }
}
