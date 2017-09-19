package io.mewbase;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;

import io.mewbase.server.CommandHandler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
@RunWith(VertxUnitRunner.class)
public class RESTAdaptorTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(RESTAdaptorTest.class);

    // TODO - Use new Event Source
    // protected Producer prod;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        installInsertProjection();
        //prod = client.createProducer(TEST_CHANNEL_1);
    }


    protected void setupChannelsAndBinders() throws Exception {
       // server.createChannel(TEST_CHANNEL_1).get();
       // server.createBinder(TEST_BINDER1).get();
    }

    @Test
    public void testCommandWithPathParam(TestContext testContext) throws Exception {
        String commandName = "testcommand";
        String customerID = "customer123";

//        CommandHandler handler = server.buildCommandHandler(commandName)
////                .emittingTo(TEST_CHANNEL_1)
////                .as((command, context) -> {
////                    testContext.assertEquals(customerID, command.getBsonObject("pathParams").getString("customerID"));
////                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
////                    context.complete();
////                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());

    //    Async async = testContext.async(2);

//        Consumer<ClientDelivery> subHandler = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals("foobar", event.getString("eventField"));
//            async.complete();
//        };

        //client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler).get();

        server.exposeCommand(commandName, "/orders/:customerID", HttpMethod.POST);

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

        HttpClient httpClient = vertx.createHttpClient();
        HttpClientRequest req = httpClient.request(HttpMethod.POST, 8080, "localhost", "/orders/" + customerID, resp -> {
            assertEquals(200, resp.statusCode());
           // async.complete();
        });
        req.putHeader("content-type", "text/json");
        req.end(sentCommand.encode());
    }

    @Test
    public void testSimpleCommand(TestContext testContext) throws Exception {
        String commandName = "testcommand";
//        CommandHandler handler = server.buildCommandHandler(commandName)
////                .emittingTo(TEST_CHANNEL_1)
////                .as((command, context) -> {
////                    testContext.assertNull(command.getBsonObject("pathParams"));
////                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
////                    context.complete();
////                })
//                .create();
//
//        assertNotNull(handler);
//        assertEquals(commandName, handler.getName());

  //      Async async = testContext.async(2);

//        Consumer<ClientDelivery> subHandler = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals("foobar", event.getString("eventField"));
//            async.complete();
//        };

        //client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler).get();

        server.exposeCommand(commandName, "/orders", HttpMethod.POST);

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

        HttpClient httpClient = vertx.createHttpClient();
        HttpClientRequest req = httpClient.request(HttpMethod.POST, 8080, "localhost", "/orders", resp -> {
            assertEquals(200, resp.statusCode());
           // async.complete();
        });
        req.putHeader("content-type", "text/json");
        req.end(sentCommand.encode());
    }

    //@Test
    public void testSimpleQuery(TestContext testContext) throws Exception {

        String queryName = "testQuery";

        int numDocs = 100;
        BsonArray bsonArray = new BsonArray();
        for (int i = 0; i < numDocs; i++) {
            String docID = getID(i);
            BsonObject doc = new BsonObject().put("id", docID).put("foo", "bar");
           // prod.publish(doc).get();
            bsonArray.add(doc);
        }

       // waitForDoc(numDocs - 1);

        // Setup a query
//        server.buildQuery(queryName).documentFilter((doc, ctx) -> {
//            return true;
//        }).from(TEST_BINDER1).create();

        server.exposeQuery(queryName, "/orders/");

        Async async = testContext.async();

        HttpClient httpClient = vertx.createHttpClient();
        HttpClientRequest req = httpClient.request(HttpMethod.GET, 8080, "localhost", "/orders/", resp -> {
            assertEquals(200, resp.statusCode());
            resp.bodyHandler(body -> {
                BsonArray arr = new BsonArray(new JsonArray(body.toString()));
                assertEquals(bsonArray, arr);
                async.complete();
            });
            resp.exceptionHandler(t -> t.printStackTrace());
        });
        req.exceptionHandler(t -> {
            t.printStackTrace();
        });

        req.putHeader("content-type", "text/json");
        req.end();

    }

    //@Test
    public void testFindByID(TestContext testContext) throws Exception {

        BsonObject doc = new BsonObject().put("id", getID(0)).put("foo", "bar");
      //  prod.publish(doc).get();
      //  waitForDoc(0);

      //  server.exposeFindByID(TEST_BINDER1, "/orders/:id");

        Async async = testContext.async();

        HttpClient httpClient = vertx.createHttpClient();
        HttpClientRequest req = httpClient.request(HttpMethod.GET, 8080, "localhost",
                "/orders/" + getID(0), resp -> {
                    assertEquals(200, resp.statusCode());
                    resp.bodyHandler(body -> {
                        BsonObject received = new BsonObject(new JsonObject(body.toString()));
                        assertEquals(doc, received);
                        async.complete();
                    });
                    resp.exceptionHandler(t -> t.printStackTrace());
                });
        req.exceptionHandler(t -> {
            t.printStackTrace();
        });

        req.putHeader("content-type", "text/json");
        req.end();
    }

//    protected BsonObject waitForDoc(int docID) {
//        // Wait until docs are inserted
////        return waitForNonNull(() -> {
////            try {
////                return client.findByID(TEST_BINDER1, getID(docID)).get();
////            } catch (Exception e) {
////                throw new RuntimeException(e);
////            }
////        });
//        assert
//    }

    protected void installInsertProjection() {
//        server.buildProjection("testproj").projecting(TEST_CHANNEL_1).onto(TEST_BINDER1).filteredBy(ev -> true)
//                .identifiedBy(ev -> ev.getString("id"))
//                .as((basket, del) -> del.event()).create();
    }


    protected String getID(int id) {
        return String.format("id-%05d", id);
    }


}
