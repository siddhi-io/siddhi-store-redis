/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.store.redis.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.input.InputHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InsertIntoRedisTableTestCase {
    private static final Logger log = LoggerFactory.getLogger(InsertIntoRedisTableTestCase.class);
    private static final String TABLE_NAME = "fooTable";

    @BeforeClass
    public static void startTest() {
        log.info("== Redis Table INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Redis Table INSERT tests completed ==");
    }

    @BeforeMethod
    public void init() throws ConnectionUnavailableException {
        RedisTestUtils.cleanRedisDatabase();
    }

    @Test
    public void insertIntoRedisTableTest1() throws InterruptedException, ConnectionUnavailableException {
        log.info("insertIntoRedisTableTest 1 - insert data into a redis table with a primary key and index column");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (name string, amount double);" +
                "@store(type='redis', host='localhost', " +
                "port='6379', table.name='fooTable', password= 'root')" +
                "@PrimaryKey('name')" +
                "@Index('amount')" +
                "define table fooTable(name string, amount double); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into fooTable; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100.0});
        stockStream.send(new Object[]{"IBM", 1001});
        stockStream.send(new Object[]{"MSFT", 1001.0});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 6, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRedisTableTest2() throws InterruptedException, ConnectionUnavailableException {
        log.info("insertIntoRedisTableTest 2 - insert data into a redis table with a primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (name string, amount double);" +
                "@store(type='redis', host='localhost', " +
                "port='6379', table.name='fooTable', password= 'root')" +
                "@PrimaryKey('name')" +
                "define table fooTable(name string, amount double); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into fooTable; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100.0});
        stockStream.send(new Object[]{"IBM", 1001});
        stockStream.send(new Object[]{"MSFT", 1001.0});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRedisTableTest3() throws InterruptedException, ConnectionUnavailableException {
        log.info("insertIntoRedisTableTest 3 - insert data into a redis table without a primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (name string, amount double);" +
                "@store(type='redis', host='localhost', " +
                "port='6379', table.name='fooTable', password= 'root')" +
                "define table fooTable(name string, amount double); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into fooTable; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100.0});
        stockStream.send(new Object[]{"IBM", 1001});
        stockStream.send(new Object[]{"MSFT", 1001.0});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void insertIntoRedisTableTest4() throws InterruptedException, ConnectionUnavailableException {
        log.info("insertIntoRedisTableTest 4 - insert data into a redis table with an index column");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (name string, amount double);" +
                "@store(type='redis', host='localhost', " +
                "port='6379', table.name='fooTable', password= 'root')" +
                "@index('amount')" +
                "define table fooTable(name string, amount double); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into fooTable; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 100.0});
        stockStream.send(new Object[]{"IBM", 1001});
        stockStream.send(new Object[]{"MSFT", 1001.0});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 6, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoRedisTableTest5() throws InterruptedException, ConnectionUnavailableException {
        log.info("insertIntoRedisTableTest 5 - insert data into a redis table with ttl and without a primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
            "define stream StockStream (name string, amount double);" +
            "@store(type='redis', host='localhost', ttl.seconds='3', " +
            "port='6379', table.name='fooTable', password= 'root')" +
            "define table fooTable(name string, amount double); ";
        String query = "" +
            "@info(name = 'query1') " +
            "from StockStream " +
            "insert into fooTable; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100.0});
        stockStream.send(new Object[]{"IBM", 1001});
        stockStream.send(new Object[]{"MSFT", 1001.0});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Thread.sleep(5000);
        int totalRowsInTablePostTtl = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        Assert.assertEquals(totalRowsInTablePostTtl, 0, "TTL failure");
        siddhiAppRuntime.shutdown();
    }
}
