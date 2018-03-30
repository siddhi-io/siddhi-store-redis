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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.sql.SQLException;

public class UpdateOrInsertRedisTableTestCase {
    private static final Logger log = Logger.getLogger(UpdateOrInsertRedisTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private static final String TABLE_NAME = "StockTable";

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
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
    public void updateOrInsertRedisTableTest1() throws InterruptedException, SQLException {
        log.info("updateOrInsertRedisTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379,password='root'" +
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
                "   on StockTable.symbol=='IBM';" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 55.6F, 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 75.6F, 100L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 57.6F, 100L}, event.getData());
                                break;
                            case 4:
                                AssertJUnit.assertArrayEquals(new Object[]{"GOOG", 10.6F, 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(4, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        checkStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        Thread.sleep(3000);

        AssertJUnit.assertEquals("Number of success events", 4, inEventCount);
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);

        siddhiAppRuntime.shutdown();

    }

    @Test(expectedExceptions = OperationNotSupportedException.class)
    public void updateOrInsertRedisTableTest2() throws InterruptedException, SQLException {
        log.info("updateOrInsertRedisTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379,password='root'" +
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
                "   on StockTable.price == price; " +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        checkStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        siddhiAppRuntime.shutdown();

    }

}
