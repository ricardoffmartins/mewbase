package io.mewbase;

import io.mewbase.bson.BsonObject;

import io.mewbase.server.CommandHandler;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class CQRSTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(CQRSTest.class);


    protected void setupChannelsAndBinders() throws Exception {
       // server.createChannel(TEST_CHANNEL_1).get();
       // server.createChannel(TEST_CHANNEL_2).get();
    }

    //@Test
    public void testCommandOK(TestContext testContext) throws Exception {

        String commandName = "testcommand";

        CommandHandler handler = server.buildCommandHandler(commandName)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
//                    context.complete();
//                })
                .create();

        assertNotNull(handler);
        assertEquals(commandName, handler.getName());

        Async async = testContext.async();

//        Consumer<ClientDelivery> subHandler = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals("foobar", event.getString("eventField"));
//            async.complete();
//        };

       // client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler).get();
       // BsonObject sentCommand = new BsonObject().put("commandField", "foobar");
       // client.sendCommand(commandName, sentCommand).get();

    }


    @Test
    public void testCommandFail(TestContext testContext) throws Exception {

        String commandName = "testcommand";

        CommandHandler handler = server.buildCommandHandler(commandName)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    context.completeExceptionally(new Exception("rejected"));
//                })
                .create();

        assertNotNull(handler);
        assertEquals(commandName, handler.getName());

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//            client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
//            MewException me = (MewException)e.getCause();
//            // OK
//            testContext.assertEquals("rejected", me.getMessage());
//            testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
//        }

    }

    @Test
    public void testCommandHandlerThrowsException(TestContext testContext) throws Exception {

        String commandName = "testcommand";

        CommandHandler handler = server.buildCommandHandler(commandName)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    //throw new Exception("oops!");
//                })
                .create();

        assertNotNull(handler);
        assertEquals(commandName, handler.getName());

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//           // client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
//            //MewException me = (MewException)e.getCause();
//            // OK
//            //testContext.assertEquals("oops!", me.getMessage());
//            //testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
//        }

    }

    @Test
    public void testNoSuchCommandhandler(TestContext testContext) throws Exception {

        String commandName = "nocommand";

        BsonObject sentCommand = new BsonObject().put("commandField", "foobar");

//        try {
//            //client.sendCommand(commandName, sentCommand).get();
//            fail("Should throw exception");
//        } catch (ExecutionException e) {
            //MewException me = (MewException)e.getCause();
            // OK
            //testContext.assertEquals("No handler for nocommand", me.getMessage());
            //testContext.assertEquals(Client.ERR_COMMAND_NOT_PROCESSED, me.getErrorCode());
       // }

    }

   // @Test
    public void testMultipleCommandHandlers(TestContext testContext) throws Exception {

        String commandName1 = "testcommand1";

        CommandHandler handler1 = server.buildCommandHandler(commandName1)
//                .emittingTo(TEST_CHANNEL_1)
//                .as((command, context) -> {
//                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
//                    context.complete();
//                })
                .create();

        assertNotNull(handler1);
        assertEquals(commandName1, handler1.getName());

        String commandName2 = "testcommand2";

        CommandHandler handler2 = server.buildCommandHandler(commandName2)
//                .emittingTo(TEST_CHANNEL_2)
//                .as((command, context) -> {
//                    context.publishEvent(new BsonObject().put("eventField", command.getString("commandField")));
//                    context.complete();
//                })
                .create();

        assertNotNull(handler2);
        assertEquals(commandName2, handler2.getName());


        Async async1 = testContext.async();

//        Consumer<ClientDelivery> subHandler1 = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals(commandName1, event.getString("eventField"));
//            async1.complete();
//        };

        //client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1), subHandler1).get();

        Async async2 = testContext.async();

//        Consumer<ClientDelivery> subHandler2 = del -> {
//            BsonObject event = del.event();
//            testContext.assertEquals(commandName2, event.getString("eventField"));
//            async2.complete();
//        };

       // client.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_2), subHandler2).get();

        BsonObject sentCommand1 = new BsonObject().put("commandField", commandName1);
//        client.sendCommand(commandName1, sentCommand1).get();

        BsonObject sentCommand2 = new BsonObject().put("commandField", commandName2);
//        client.sendCommand(commandName2, sentCommand2).get();

    }

}
