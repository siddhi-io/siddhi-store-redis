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

import org.wso2.extension.siddhi.store.redis.beans.StoreVariable;
import org.wso2.extension.siddhi.store.redis.beans.StreamVariable;
import org.wso2.extension.siddhi.store.redis.utils.RedisTableConstants;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.query.api.definition.Attribute;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Redis iterator Class
 **/
public class RedisIterator implements RecordIterator<Object[]> {
    private Jedis jedis;
    private Map<String, String> resultMap = new HashMap<>();
    private ScanResult scanResults;
    private List<Attribute> attributes;
    private String tableName;
    private List scanResultsList = new ArrayList();
    private boolean preFetched;
    private Object[] nextValue;
    private boolean initialTraverse;
    private boolean isInitialTraverse = true;
    private BasicCompareOperation query;
    private List<String> primaryKeys;
    private List<String> indexes;
    private Object[] result;
    private StoreVariable storeVariable;
    private StreamVariable streamVariable;
    private Long stringCursor = RedisTableConstants.REDIS_DEFAULT_CURSOR;
    private Iterator<Object[]> iterator;

    public RedisIterator(Jedis jedis, List<Attribute> attributes, BasicCompareOperation query, String tableName,
                         List<String> primaryKeys, List<String> indexes) {
        this.jedis = jedis;
        this.attributes = attributes;
        this.tableName = tableName;
        this.initialTraverse = true;
        this.query = query;
        this.primaryKeys = primaryKeys;
        this.indexes = indexes;

    }

    @Override
    public void close() throws IOException {
        closeImpl();
    }

    private void closeImpl() {
        if (Objects.nonNull(resultMap)) {
            resultMap.clear();
        }
        result = null;
        scanResultsList = null;
        scanResults = null;
    }

    @Override
    public boolean hasNext() {
        if (!this.preFetched) {
            // check whether object is pre fetched
            this.nextValue = this.next();
            this.preFetched = true;
        }
        return nextValue != null;
    }

    @Override
    public Object[] next() {
        if (this.preFetched) {
            this.preFetched = false;
            Object[] record = this.nextValue;
            this.nextValue = null;
            return record;
        }
        List<Object[]> finalResult;
        if (initialTraverse) {
            finalResult = fetchResults();
            initialTraverse = false;
            this.iterator = finalResult.listIterator();
            if (finalResult.isEmpty()) {
                this.closeImpl();
                return null;
            }
        }
        if (!iterator.hasNext()) {
            scanResultsList.clear();
            finalResult = fetchResults();
            this.iterator = finalResult.listIterator();
            if (finalResult.isEmpty()) {
                this.closeImpl();
                return null;
            }
            return iterator.next();
        } else {
            return iterator.next();
        }

    }

    @Override
    public void remove() {
        //Do nothing. This is a read only iterator
    }

    private List<Object[]> fetchResults() {
        List<Object[]> resultList = new ArrayList<>();

        if (Objects.nonNull(query) &&
                !("true".equalsIgnoreCase((query.getStreamVariable()).getName()))) {
            return fetchResultsWithCondition();

        } else if (Objects.nonNull(query) &&
                ("true".equalsIgnoreCase((query.getStreamVariable()).getName()))) {

            return fetchAllResults();
        }
        return resultList;
    }

    private List<Object[]> fetchResultsWithCondition() {
        List<Object[]> resultList = new ArrayList<>();

        storeVariable = query
                .getStoreVariable();
        streamVariable = query
                .getStreamVariable();
        if (indexes.contains(storeVariable.getName())) {
            return fetchResultsByIndexedValue();
        } else if (primaryKeys.contains(storeVariable.getName())) {
            return fetchResultsByPrimaryKey();
        }
        return resultList;
    }

    private List<Object[]> fetchResultsByPrimaryKey() {
        List<Object[]> resultList = new ArrayList<>();
        if (isInitialTraverse) {
            resultMap = jedis.hgetAll(tableName + ":" + streamVariable.getName());
            if (Objects.nonNull(resultMap) && !resultMap.isEmpty()) {
                result = resultsGenerator(resultMap);
                resultList.add(result);
                isInitialTraverse = false;
            }
        } else {
            return resultList;
        }
        return resultList;
    }

    private List<Object[]> fetchResultsByIndexedValue() {
        List<Object[]> resultList = new ArrayList<>();
        ScanParams scanParams = new ScanParams();
        scanParams.count(RedisTableConstants.REDIS_BATCH_SIZE);
        if (isInitialTraverse) {
            isInitialTraverse = false;
            scanResultsList = fetchBatchOfResultsFromIndexTable(scanParams);
            if (scanResultsList.isEmpty()) {
                return resultList;
            }
        }
        if (scanResultsList.isEmpty() && stringCursor > 0) {
            scanResultsList = fetchBatchOfResultsFromIndexTable(scanParams);
            if (scanResultsList.isEmpty()) {
                return resultList;
            }
        } else if (scanResultsList.isEmpty() && stringCursor == 0) {
            return resultList;
        }
        scanResultsList.forEach(scanResult -> {
            resultMap = jedis.hgetAll(scanResult.toString());
            if (Objects.nonNull(resultMap)) {
                result = resultsGenerator(resultMap);
                resultList.add(result);
            }
        });
        return resultList;
    }

    private List fetchBatchOfResultsFromIndexTable(ScanParams scanParams) {

        scanResults = jedis.sscan(tableName + ":" + storeVariable.getName() + ":"
                + streamVariable.getName(), String.valueOf(stringCursor), scanParams);
        stringCursor = Long.valueOf(scanResults.getStringCursor());
        return scanResults.getResult();
    }

    private List<Object[]> fetchAllResults() {
        List<Object[]> resultList = new ArrayList<>();
        if (isInitialTraverse) {
            isInitialTraverse = false;
            this.scanResultsList = fetchBatchOfResultsFromTable();
            if (scanResultsList.isEmpty()) {
                return resultList;
            }
        } else if (scanResultsList.isEmpty() && stringCursor > 0) {
            this.scanResultsList = fetchBatchOfResultsFromTable();
            if (scanResultsList.isEmpty()) {
                return resultList;
            }
        } else if (scanResultsList.isEmpty() && stringCursor == 0) {
            return resultList;
        }

        scanResultsList.forEach(e -> {
            String type = jedis.type(e.toString());
            if (type.equalsIgnoreCase("hash")) {
                resultMap = jedis.hgetAll(e.toString());
                if (Objects.nonNull(resultMap)) {
                    result = resultsGenerator(resultMap);
                    resultList.add(result);
                }
            }
        });
        return resultList;
    }

    private List fetchBatchOfResultsFromTable() {
        ScanParams scanParams = new ScanParams();
        scanParams.match(tableName + ":*");
        scanParams.count(RedisTableConstants.REDIS_BATCH_SIZE);
        scanResults = jedis.scan(String.valueOf(stringCursor), scanParams);
        stringCursor = Long.valueOf(scanResults.getStringCursor());
        return scanResults.getResult();
    }


    private Object[] resultsGenerator(Map<String, String> resultMap) {
        Object[] generatedResult = new Object[attributes.size()];
        int i = 0;
        for (Attribute attribute : this.attributes) {
            switch (attribute.getType()) {
                case BOOL:
                    generatedResult[i++] = Boolean.parseBoolean(resultMap.get(attribute.getName
                            ()));
                    break;
                case INT:
                    generatedResult[i++] = Integer.parseInt(resultMap.get(attribute.getName()));
                    break;
                case LONG:
                    generatedResult[i++] = Long.parseLong(resultMap.get(attribute.getName()));
                    break;
                case FLOAT:
                    generatedResult[i++] = Float.parseFloat(resultMap.get(attribute.getName()));
                    break;
                case DOUBLE:
                    generatedResult[i++] = Double.parseDouble(resultMap.get(attribute.getName()));
                    break;
                default:
                    generatedResult[i++] = resultMap.get(attribute.getName());
                    break;
            }
        }
        return generatedResult;
    }
}
