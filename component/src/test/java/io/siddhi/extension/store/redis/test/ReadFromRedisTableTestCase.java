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

package io.siddhi.extension.store.redis.test;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.ArrayList;

public class ReadFromRedisTableTestCase {
    private static final Logger log = LoggerFactory.getLogger(ReadFromRedisTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    public static final String TABLE_NAME = "StockTable";

    static final Object[] WSO2 = new Object[] {"WSO2", 50.0F, 100L};
    static final Object[] CSC = new Object[] {"CSC", 40.F, 10L};
    static final Object[] IBM = new Object[] {"IBM", 30.F, 10L};
    
    @BeforeClass
    public static void startTest() {
        log.info("== Redis Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Redis Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() throws ConnectionUnavailableException {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        RedisTestUtils.cleanRedisDatabase();
    }

    @Test
    public void readEventRedisTableTestCase1() throws InterruptedException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase 1 - Read events using primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream searchStream (symbol string); " +
                "define stream OutputStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379'," +
                "password='root') " +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                " " +
                "@info(name = 'query2')\n" +
                "from searchStream#window.length(1) join StockTable on searchStream.symbol==StockTable.symbol " +
                "select searchStream.symbol as symbol, StockTable.price as price, " +
                "StockTable.volume as volume\n " +
                "insert into OutputStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("searchStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), WSO2);
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), IBM);
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
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
        siddhiAppRuntime.start();

        stockStream.send(WSO2);
        stockStream.send(CSC);
        stockStream.send(IBM);

        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void readEventRedisTableTestCase2() throws InterruptedException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase 2 - Read event from table using index");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream searchStream (price float); " +
                "define stream OutputStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379'," +
                "password='root')" +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                " " +
                "@info(name = 'query2')\n" +
                "from searchStream#window.length(1) join StockTable on searchStream.price==StockTable.price " +
                "select StockTable.symbol as symbol, searchStream.price as price,  " +
                "StockTable.volume as volume\n " +
                "insert into OutputStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("searchStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"CSC", 40.0F, 10L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), IBM);
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
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
        siddhiAppRuntime.start();
        stockStream.send(WSO2);
        stockStream.send(CSC);
        stockStream.send(IBM);

        searchStream.send(new Object[]{40.F});
        searchStream.send(new Object[]{30.F});

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void readEventRedisTableTestCase3() throws InterruptedException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase 3 - Read event from a table without primary key using index column");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream searchStream (price float); " +
                "define stream OutputStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379'," +
                "password='root')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                " " +
                "@info(name = 'query2')\n" +
                "from searchStream#window.length(1) join StockTable on searchStream.price==StockTable.price " +
                "select StockTable.symbol as symbol, searchStream.price as price, StockTable.volume as volume\n " +
                "insert into OutputStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("searchStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"CSC", 40.0F, 10L});
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
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
        siddhiAppRuntime.start();
        stockStream.send(WSO2);
        stockStream.send(CSC);
        stockStream.send(IBM);

        searchStream.send(new Object[]{40.F});

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }
    
    @Test
    public void readEventRedisTableTestCase4() throws InterruptedException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase 4 - Read events using primary key and ttl");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream searchStream (symbol string); " +
                "define stream OutputStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379'," +
                "ttl.seconds='5', ttl.on.read='true', password='root') " +
                "@PrimaryKey('symbol')" +
                //"@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                " " +
                "@info(name = 'query2')\n" +
                "from searchStream join StockTable on searchStream.symbol==StockTable.symbol " +
                "select searchStream.symbol as symbol, StockTable.price as price, " +
                "StockTable.volume as volume\n " +
                "insert into OutputStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("searchStream");
        ArrayList<Object[]> recData = new ArrayList();
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                inEventCount++;
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        recData.add(event.getData());
                    }
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });
        siddhiAppRuntime.start();
        
        stockStream.send(WSO2);
        stockStream.send(CSC);
        stockStream.send(IBM);
        
        log.info("inserts done");

        Thread.sleep(4000);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});

        log.info("select wso2 / ibm done");
        Assert.assertEquals(recData.get(0), WSO2);
        Assert.assertEquals(recData.get(1), IBM);
        Assert.assertEquals(recData.size(), 2);
        recData.clear();
        
        Thread.sleep(4000);
        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});
        searchStream.send(new Object[]{"CSC"});
        
        Assert.assertEquals(recData.get(0), WSO2);
        Assert.assertEquals(recData.get(1), IBM);
        Assert.assertEquals(recData.size(), 2);
        
        Assert.assertEquals(recData.size(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    
    @Test
    public void readEventRedisTableTestCase5() throws InterruptedException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase 5 - Read events using index key and ttl");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream searchStream (price float); " +
                "define stream OutputStream (symbol string, price float, volume long); " +
                "@Store(type='redis', table.name='" + TABLE_NAME + "', host= 'localhost',port='6379'," +
                "ttl.seconds='5', ttl.on.read='true', password='root') " +
                "@PrimaryKey('symbol')" +
                "@index('price')" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                " " +
                "@info(name = 'query2')\n" +
                "from searchStream join StockTable on searchStream.price==StockTable.price " +
                "select StockTable.symbol as symbol, searchStream.price as price, " +
                "StockTable.volume as volume\n " +
                "insert into OutputStream; ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler searchStream = siddhiAppRuntime.getInputHandler("searchStream");
        ArrayList<Object[]> recData = new ArrayList();
        
        
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
          @Override
          public void receive(Event[] events) {
            inEventCount++;
            eventArrived = true;
            for (Event event : events) {
              recData.add(event.getData());
            }
          }
        });
        
        siddhiAppRuntime.start();
        
        stockStream.send(WSO2);
        stockStream.send(CSC);
        stockStream.send(IBM);
        
        log.info("inserts done");

        Thread.sleep(4000);
        searchStream.send(new Object[]{WSO2[1]});
        searchStream.send(new Object[]{IBM[1]});

        log.info("select wso2 / ibm done");
        Assert.assertEquals(recData.get(0), WSO2);
        Assert.assertEquals(recData.get(1), IBM);
        Assert.assertEquals(recData.size(), 2);
        Assert.assertEquals(eventArrived, true);
        
        // reset
        recData.clear();
        eventArrived = false;
        // sleep past the original ttl 
        Thread.sleep(4000);
        // still in the table thanks to the last read
        searchStream.send(new Object[]{WSO2[1]});
        searchStream.send(new Object[]{IBM[1]});
        
        // was not read, in the last 8 secs, it must be gone
        searchStream.send(new Object[]{CSC[1]});
        
        Assert.assertEquals(recData.get(0), WSO2);
        Assert.assertEquals(recData.get(1), IBM);
        Assert.assertEquals(recData.size(), 2);
        
        Assert.assertEquals(recData.size(), 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }
}
