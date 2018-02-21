package org.wso2.extension.siddhi.store.redis;

import org.wso2.extension.siddhi.store.redis.exceptions.RedisIteratorException;
import org.wso2.extension.siddhi.store.redis.utils.RedisTableConstants;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.query.api.definition.Attribute;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Redis iterator Class
 **/
public class RedisIterator implements RecordIterator<Object[]> {
    private Jedis jedis;
    private Map<String, String> resultMap = new HashMap<>();
    private ScanResult scanResult;
    private List<Attribute> attributes;
    private String tableName;
    private List scanResultsList;
    private boolean preFetched;
    private Object[] nextValue;
    private boolean initialTraverse;
    private List<String> query;

    public RedisIterator(Jedis jedis, List<Attribute> attributes, List<String> query, String tableName) {
        this.jedis = jedis;
        this.attributes = attributes;
        this.tableName = tableName;
        this.initialTraverse = true;
        this.query = query;

    }

    @Override
    public void close() throws IOException {
        resultMap.clear();
        scanResultsList.clear();
        scanResult = null;
    }

    @Override
    public boolean hasNext() {
        if (!this.preFetched) {
            this.nextValue = this.next();
            this.preFetched = true;
        }
        // TODO: 2/14/18 Check whether sending null is ok rather than sending a empty object[]
        return nextValue.length == 0;
    }

    @Override
    public Object[] next() {
        List<Object[]> resultsList = new ArrayList<>();
        if (this.preFetched) {
            this.preFetched = false;
            Object[] result = this.nextValue;
            this.nextValue = null;
            return result;
        }
        try {
            if (initialTraverse) {
                resultsList = fetchResults();
                initialTraverse = false;
            }
            if (Objects.nonNull(resultsList) && resultsList.iterator().hasNext()) {
                return resultsList.toArray();
            } else {
                this.close();
                return new Object[0];
            }
        } catch (Exception e) {
            throw new RedisIteratorException("Error while retrieving records from table " + this.tableName + " : " +
                    "" + e.getMessage(), e);
        }

    }

    @Override
    public void remove() {
        //Do nothing. This is a read only iterator
    }

    @Override
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    private List<Object[]> fetchResults() {
        List<Object[]> resultList = new ArrayList<>();
        ScanParams scanParams = new ScanParams();
        scanParams.match(tableName + ":*");
        scanParams.count(RedisTableConstants.REDIS_BATCH_SIZE);
        this.scanResult = jedis.scan("0", scanParams);
        this.scanResultsList = scanResult.getResult();
        while (Long.parseLong(this.scanResult.getStringCursor()) > 0) {
            this.scanResult = jedis.scan(scanResult.getStringCursor(), scanParams);
            this.scanResultsList.add(scanResult.getResult());
        }


        if (scanResultsList.iterator().hasNext()) {
            resultMap = jedis.hgetAll(scanResultsList.iterator().next().toString());
            resultList = filterResult(resultMap, query);
        }
        return resultList;
    }

    private List<Object[]> filterResult(Map<String, String> resultsMap, List<String> query) {
        List<Object[]> filteredResults = new ArrayList<>();
        Object[] result = new Object[attributes.size()];

        int i = 0;
        for (String subQuery : query) {
            if (query.size() > 3) {
                i++;
                if (subQuery.equals(RedisTableConstants.JAVA_AND)) {
                    break;
                } else if (subQuery.equals(RedisTableConstants.JAVA_OR)) {
                    break;
                } else if (subQuery.equals(RedisTableConstants.JAVA_NOT)) {
                    break;
                } else if (subQuery.equals(RedisTableConstants.JAVA_NULL)) {
                    break;
                }
            } else {
                i++;
                if (subQuery.equals(RedisTableConstants.EQUAL)) {
                    switch (fetchAttributeType(query.get(i - 1))) {
                        case STRING:
                            if (resultsMap.get(query.get(i - 1)).equals(query.get(i + 1))) {
                            }
                            break;
                        default:
                            if (resultsMap.get(query.get(i - 1)) == query.get(i + 1)) {
                                break;
                            }
                    }

                }
            }
        }

        return filteredResults;
    }


//        while (Objects.nonNull(query)) {
////            ListIterator<String> queryIterator = query.listIterator();
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
////            switch (queryIterator.next()) {
////                case RedisTableConstants.JAVA_AND:
////                    String operator = query.get(queryIterator.previousIndex() - 1);
////                    switch (operator) {
////                        case RedisTableConstants.EQUAL:
////                            break;
////                        case RedisTableConstants.GREATER_THAN:
////                            break;
////                        case RedisTableConstants.LESS_THAN:
////                            break;
////                        case RedisTableConstants.LESS_THAN_EQUAL:
////                            break;
////                        case RedisTableConstants.GREATER_THAN_EQUAL:
////                            break;
////                    }
////                    break;
////                case RedisTableConstants.JAVA_OR:
////                    break;
////                case RedisTableConstants.JAVA_NOT:
////                    break;
////                case RedisTableConstants.JAVA_NULL:
////                    break;
////                default:
////                    break;
////            }
//        }

    private Attribute.Type fetchAttributeType(String field) {
        for (Attribute attribute : attributes) {
            if (attribute.getName().equals(field)) {
                return attribute.getType();
            }
        }
        return null;
    }
//
//    private List<Object[]> resultFilter(List<Object[]> resultList) {
//        List<Object[]> filteredResuls = new ArrayList<>();
//        return filteredResuls;
//    }
//    }
    }
}