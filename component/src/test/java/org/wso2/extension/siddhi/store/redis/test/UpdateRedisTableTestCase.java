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
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;


public class UpdateRedisTableTestCase {
    private static final Logger log = LoggerFactory.getLogger(UpdateRedisTableTestCase.class);
    private AtomicInteger inEventCount = new AtomicInteger();
    private boolean eventArrived;
    private int removeEventCount;
    private static final String TABLE_NAME = "fooTable";

    @BeforeMethod
    public void init() throws ConnectionUnavailableException {
        inEventCount.set(0);
        removeEventCount = 0;
        RedisTestUtils.cleanRedisDatabase();
        eventArrived = false;
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Redis Table UPDATE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Redis Table UPDATE tests completed ==");
    }

    @Test
    public void updateFromTableTest1() throws InterruptedException {
        //Check for update event data in Redis table when a primary key condition is true.
        log.info("updateFromTableTest 1 - update event data in Redis table when a primary key condition is true. ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price string, volume long); " +
                "define stream UpdateStockStream (symbol string, price string, volume long); " +
                "define stream CheckStockStream (symbol string, price string, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price string, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +

                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol); \n" +
                "@info(name = 'query3') " +
                "from CheckStockStream[ (price==StockTable.price) in StockTable] " +
                "insert into OutStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", "75.6", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", "57.6", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount.get());
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

        stockStream.send(new Object[]{"WSO2", "55.6", 100L});
        stockStream.send(new Object[]{"IBM", "75.6", 100L});

        updateStockStream.send(new Object[]{"WSO2", "57.6", 100L});

        checkStockStream.send(new Object[]{"IBM", "75.6", 100L});
        checkStockStream.send(new Object[]{"WSO2", "57.6", 100L});

        SiddhiTestHelper.waitForEvents(1000, 2, inEventCount, 6000);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest1")
    public void updateFromTableTest2() throws InterruptedException {
        //Check for update event data in Redis table when index key are not present;
        log.info("updateFromTableTest 2 - update event data in Redis table when index key are not present. ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price string, volume long); " +
                "define stream UpdateStockStream (symbol string, price string, volume long); " +
                "define stream CheckStockStream (symbol string, price string, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price string, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update StockTable " +
                "set StockTable.volume = volume" +
                "   on StockTable.price == price;" +
                "" +
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
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", 75.6, 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"AWS", 57.6, 200L}, event.getData());
                                break;
                            case 3:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", 57.6, 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(3, inEventCount.get());
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

        stockStream.send(new Object[]{"WSO2", 55.6, 100L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"AWS", 57.6, 200L});

        updateStockStream.send(new Object[]{"WSO2", 57.6, 100L});

        checkStockStream.send(new Object[]{"IBM", 75.6, 100L});
        checkStockStream.send(new Object[]{"AWS", 57.6, 200L});
        checkStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        await().atMost(5, TimeUnit.SECONDS);

        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest2", expectedExceptions = SiddhiAppCreationException.class)
    public void updateFromTableTest3() throws InterruptedException {
        log.info("updateFromTableTest 3 - update event data in Redis table when a primary key condition is true. ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price string, volume long); " +
                "define stream UpdateStockStream (symbol string, price string, volume long); " +
                "define stream CheckStockStream (symbol string, price string, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379',password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price string, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +

                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "set StockTable.price = price +10" +
                "on (StockTable.symbol == symbol);" +
                "" +
                "@info(name = 'query3') " +
                "from CheckStockStream[ (price==StockTable.price) in StockTable] " +
                "insert into OutStream;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                AssertJUnit.assertArrayEquals(new Object[]{"IBM", "75.6", 100L}, event.getData());
                                break;
                            case 2:
                                AssertJUnit.assertArrayEquals(new Object[]{"WSO2", "100.0", 100L}, event.getData());
                                break;
                            default:
                                AssertJUnit.assertSame(2, inEventCount.get());
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

        stockStream.send(new Object[]{"WSO2", "55.6", 100L});
        stockStream.send(new Object[]{"IBM", "75.6", 100L});

        updateStockStream.send(new Object[]{"WSO2", "90.0", 50L});

        checkStockStream.send(new Object[]{"IBM", "75.6", 100L});
        checkStockStream.send(new Object[]{"WSO2", "57.6", 100L});

        await().atMost(5, TimeUnit.SECONDS);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertEquals("Number of remove events", 0, removeEventCount);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
