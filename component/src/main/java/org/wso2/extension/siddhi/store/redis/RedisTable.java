package org.wso2.extension.siddhi.store.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.extension.siddhi.store.redis.exceptions.RedisTableException;
import org.wso2.extension.siddhi.store.redis.utils.RedisTableConstants;
import org.wso2.extension.siddhi.store.redis.utils.RedisTableUtils;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
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
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "@store(type='redis',hoslogt='localhost',port=6379, password='root',table" +
                                ".name='fooTable')" +
                                "define table fooTable(time long, date String)",
                        description = "above collection will create a redis table with the name FooTable"
                )
        }
)

public class RedisTable extends AbstractRecordTable {
    private static final Log LOG = LogFactory.getLog(RedisTable.class);
    private List<Attribute> attributes;
    private List<String> primaryKeys;
    private Map<String, Integer> primaryKeyLocation;
    private boolean schemaUpdatedOnce;
    private boolean connectedOnce;
    private JedisPool jedisPool;
    private String host = "localhost";
    private String password;
    private int port = RedisTableConstants.DEFAULT_PORT;
    private Annotation primaryKeyAnnotation;
    private Annotation storeAnnotation;
    private Annotation indexingAnnotation;
    private String tableName;
    private Jedis jedis;
    private List<String> indexing;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributes = tableDefinition.getAttributeList();
        this.schemaUpdatedOnce = false;
        this.connectedOnce = false;

        primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        storeAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_STORE, tableDefinition
                .getAnnotations());
        indexingAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX, tableDefinition
                .getAnnotations());

        if (Objects.nonNull(primaryKeyAnnotation)) {
            this.primaryKeys = new ArrayList<>();
            List<Element> primaryKeyElements = primaryKeyAnnotation.getElements();
            primaryKeyElements.forEach(element -> this.primaryKeys.add(element.getValue().trim()));
        }
        if (Objects.nonNull(indexingAnnotation)) {
            this.indexing = new ArrayList<>();
            List<Element> indexingElements = indexingAnnotation.getElements();
            indexingElements.forEach(element -> this.indexing.add(element.getValue().trim()));
        }

        if (Objects.nonNull(storeAnnotation)) {
            host = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_HOST);

            if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT))) {
                port = Integer.parseInt(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PORT));
            }

            if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD))) {
                password = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_PASSWORD);
            }
            if (Objects.nonNull(storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_TABLE_NAME))) {
                tableName = storeAnnotation.getElement(RedisTableConstants.ANNOTATION_ELEMENT_TABLE_NAME);
            } else {
                tableName = tableDefinition.getId();
            }
        }

    }

    @Override
    protected void add(List<Object[]> records) {
        Map<String, String> attributeMap = new HashMap<>();
        StringBuilder builder = new StringBuilder(tableName);
//        StringBuilder indexMatabuilder = new StringBuilder(tableName);
        try {
            for (Object[] record : records) {
                int i = 0;
                for (Attribute attribute : attributes) {
                    attributeMap.put(attribute.getName(), record[i++].toString());
                }
                if (Objects.nonNull(primaryKeys) && !primaryKeys.isEmpty()) {

                    for (String primaryKey : primaryKeys) {
                        builder.append(":").append(attributeMap.get(primaryKey));
                    }
                    jedis.hmset(builder.toString(), attributeMap);
                    if (Objects.nonNull(indexing)) {
                    }
                } else {
                    String id = RedisTableUtils.generateRecordID();
                    jedis.hmset(id, attributeMap);
                }
            }
        } catch (JedisException e) {
            LOG.error("Error while inserting records to Redis Event Table" + e.getMessage(), e);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {

        try {
            List<String> condition = RedisTableUtils.resolveCondition((RedisCompliedCondition) compiledCondition,
                    findConditionParameterMap);
            return new RedisIterator(jedis, attributes, condition, tableName);
        } catch (Exception e) {
            throw new RedisTableException("Error while searching the records in Redis Event Table : " + tableName, e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        return false;
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {

    }

    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions,
                          List<Map<String, Object>> updateSetParameterMaps) throws ConnectionUnavailableException {

    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition,
                               List<Map<String, Object>> updateConditionParameterMaps,
                               Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps,
                               List<Object[]> addingRecords)
            throws ConnectionUnavailableException {

    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        RedisConditionVisitor visitor = new RedisConditionVisitor();
        expressionBuilder.build(visitor);
        return new RedisCompliedCondition(visitor.returnCondition());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        RedisConditionVisitor visitor = new RedisConditionVisitor();
        expressionBuilder.build(visitor);
        return new RedisCompliedCondition(visitor.returnCondition());
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        try {
            if (jedisPool == null) {
                jedisPool = new JedisPool(new JedisPoolConfig(), host, port);
                jedis = jedisPool.getResource();
                if (Objects.nonNull(password)) {
                    jedis.auth(password);
                }
            }
        } catch (Exception e) {
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
            if (Objects.nonNull(jedisPool)&&!jedisPool.isClosed()) {
                jedisPool.close();
            }
        } catch (Exception e)

        {
            LOG.error("Error while closing the redis client for table: " + tableName + " : ", e);
        }

    }

    @Override
    protected void destroy() {
        try {
            jedisPool.destroy();
        } catch (Exception e) {
            LOG.info("Error while closing the redis client for table: " + tableName + " : " + e.getMessage(), e);
        }
    }
}
