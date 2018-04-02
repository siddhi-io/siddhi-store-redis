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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class UpdateOrInsertRedisTableTestCase {
    private static final Logger log = LoggerFactory.getLogger(UpdateOrInsertRedisTableTestCase.class);
    private static final String TABLE_NAME = "StockTable";
    private AtomicInteger count = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Redis Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Redis Table UPDATE/INSERT tests completed ==");
    }

    @Test
    public void updateOrInsertRedisTableTest1() throws InterruptedException, SQLException,
            ConnectionUnavailableException {
        log.info("updateOrInsertRedisTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "on StockTable.symbol=='GOOG';";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"FB", 57.6F, 100L});

        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        Thread.sleep(5000);

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 8, "UpdateOrInsert failed");
        siddhiAppRuntime.shutdown();

        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();
    }

    @Test
    public void updateOrInsertRedisTableTest2() throws InterruptedException, SQLException,
            ConnectionUnavailableException {
        log.info("updateOrInsertRedisTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "   on StockTable.symbol == symbol; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOG", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 20.3F, 50L});
        Thread.sleep(3000);


        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 5, "UpdateOrInsert failed");
        siddhiAppRuntime.shutdown();

        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void updateOrInsertRedisTableTest3() throws InterruptedException, SQLException,
            ConnectionUnavailableException {
        log.info("updateOrInsertRedisTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream#window.timeBatch(1 sec) " +
                "update or insert into StockTable " +
                "set StockTable.volume = volume +10 " +
                "   on StockTable.price == price; ";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOG", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 20.3F, 50L});

        int totalRowsInTable = RedisTestUtils.getRowsFromTable(TABLE_NAME);
        Assert.assertEquals(totalRowsInTable, 5, "UpdateOrInsert failed");
        siddhiAppRuntime.shutdown();

        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();
    }
}
