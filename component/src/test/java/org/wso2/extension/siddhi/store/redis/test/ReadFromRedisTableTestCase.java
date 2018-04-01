package org.wso2.extension.siddhi.store.redis.test;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class ReadFromRedisTableTestCase {
    private static final Logger log = Logger.getLogger(ReadFromRedisTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    public static final String TABLE_NAME = "StockTable";

    @BeforeClass
    public static void startTest() {
        log.info("== Redis Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Redis Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void readEventRedisTableTestCase1() throws InterruptedException, ConnectionUnavailableException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase1");
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 50.0F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 30.0F, 10L});
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

        stockStream.send(new Object[]{"WSO2", 50.0F, 100L});
        stockStream.send(new Object[]{"CSC", 40.F, 10L});
        stockStream.send(new Object[]{"IBM", 30.F, 10L});

        searchStream.send(new Object[]{"WSO2"});
        searchStream.send(new Object[]{"IBM"});

        Thread.sleep(3000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();

    }

    @Test
    public void readEventRedisTableTestCase2() throws InterruptedException, ConnectionUnavailableException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase2");
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 30.0F, 10L});
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

        stockStream.send(new Object[]{"WSO2", 50.0F, 100L});
        stockStream.send(new Object[]{"CSC", 40.F, 10L});
        stockStream.send(new Object[]{"IBM", 30.F, 10L});

        searchStream.send(new Object[]{40.F});
        searchStream.send(new Object[]{30.F});

        Thread.sleep(3000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();
    }

    @Test
    public void readEventRedisTableTestCase3() throws InterruptedException, ConnectionUnavailableException {
        //Read events from a Redis table successfully
        log.info("readEventRedisTableTestCase3");
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

        stockStream.send(new Object[]{"WSO2", 50.0F, 100L});
        stockStream.send(new Object[]{"CSC", 40.F, 10L});
        stockStream.send(new Object[]{"IBM", 30.F, 10L});

        searchStream.send(new Object[]{40.F});

        Thread.sleep(3000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");

        siddhiAppRuntime.shutdown();
        RedisTestUtils.cleanRedisDatabase();


    }
}
