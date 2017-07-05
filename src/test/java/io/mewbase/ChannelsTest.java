package io.mewbase;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.mewbase.client.*;
import io.mewbase.common.SubDescriptor;
import io.mewbase.log.HeaderOpsTest;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.*;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class ChannelsTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(ChannelsTest.class);

    // Patching this in to catch the log output
    class TestAppender extends AppenderSkeleton {
        private final List<LoggingEvent> log = new ArrayList<LoggingEvent>();

        @Override
        public boolean requiresLayout() {
            return false;
        }

        @Override
        protected void append(final LoggingEvent loggingEvent) {
            log.add(loggingEvent);
        }

        @Override
        public void close() {
        }

        public List<LoggingEvent> getLog() {
            return new ArrayList<LoggingEvent>(log);
        }
    }


    private TestAppender patchTestAppender() {
        final TestAppender appender = new TestAppender();
        final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getRootLogger();
        logger.addAppender(appender);
        return appender;
    }

    private void removeTestAppender(TestAppender appender) {
        final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getRootLogger();
        logger.removeAppender(appender);
    }



    @Override
    protected void setupChannelsAndBinders() throws Exception {
        server.createChannel(TEST_CHANNEL_1).get();
    }

    @Test
    public void testListChannels() throws Exception {
        int numChannels = 10;
        CompletableFuture[] all = new CompletableFuture[numChannels];
        for (int i = 0; i < numChannels; i++) {
            all[i] = server.createChannel("testchannel" + i);
        }
        CompletableFuture.allOf(all).get();
        BsonArray channels1 = client.listChannels().get();

        Set<String> channelsSet1 = new HashSet<>(channels1.getList());
        for (int i = 0; i < numChannels; i++) {
            assertTrue(channelsSet1.contains("testchannel" + i));
        }

        final String otherChannelName = "someotherchannel";

        // Create a new one
        server.createChannel(otherChannelName).get();
        BsonArray channels2 = client.listChannels().get();
        Set<String> channelsSet2 = new HashSet<>(channels2.getList());
        assertTrue(channelsSet2.contains(otherChannelName));
        assertEquals(channelsSet1.size() + 1, channelsSet2.size());

    }

    @Test
    public void testCreateChannel() throws Exception {
        final String channelName = "somechannel";
        CompletableFuture<Boolean> cf = client.createChannel(channelName);
        assertTrue(cf.get());

        List<String> channelNames = server.listChannels();
        Set<String> channelsSet = new HashSet<>(channelNames);
        assertTrue(channelsSet.contains(channelName));

        CompletableFuture<Boolean> cf2 = client.createChannel(channelName);
        assertFalse(cf2.get());
    }

    @Test
    public void testPublishNonExistentChannel() throws Exception {
        String channel = "nosuchchannel";
        Producer prod = client.createProducer(channel);
        try {
            prod.publish(new BsonObject()).get();
            fail("Should throw exception");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof MewException);
            MewException mcause = (MewException)cause;
            assertEquals("no such channel " + channel, mcause.getMessage());
            Assert.assertEquals(Client.ERR_NO_SUCH_CHANNEL, mcause.getErrorCode());
        }
    }

    @Test
    public void testSubscribeNonExistentChannel() throws Exception {
        String channel = "nosuchchannel";
        try {
            client.subscribe(new SubDescriptor().setChannel(channel), del -> {
            }).get();
            fail("Should throw exception");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof MewException);
            MewException mcause = (MewException)cause;
            assertEquals("no such channel " + channel, mcause.getMessage());
            assertEquals(Client.ERR_NO_SUCH_CHANNEL, mcause.getErrorCode());
        }
    }

    @Test
    //@Repeat(value = 10000)
    public void testSimplePubSub(TestContext context) throws Exception {
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        Async async = context.async();
        long now = System.currentTimeMillis();
        BsonObject sent = new BsonObject().put("foo", "bar");

        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            context.assertEquals(0l, re.channelPos());
            context.assertTrue(re.timeStamp() >= now);
            BsonObject event = re.event();
            context.assertEquals(sent, event);
            async.complete();
        };

        CompletableFuture<Subscription> fut = client.subscribe(descriptor, handler);
        Subscription sub = fut.get();
        prod.publish(sent).get();

    }

    @Test
    //@Repeat(value = 10000)
    public void testSubscribeRetro(TestContext context) throws Exception {
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        int numEvents = 10;

        Set<Integer> writtenNumbers = new HashSet<Integer>(numEvents);
        for (int i = 0; i < numEvents; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            writtenNumbers.add(i);
            CompletableFuture<Void> cf = prod.publish(event);
            if (i == numEvents - 1) {
                cf.get();
            }
        }
        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setStartEventNum(0);

        Async async = context.async();
        AtomicLong lastPos = new AtomicLong(-1);
        AtomicInteger receivedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            long last = lastPos.get();
            context.assertTrue(re.channelPos() > last);
            lastPos.set(re.channelPos());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            // no order assertion but we can assert once and once only
            writtenNumbers.remove( event.getInteger("num"));
            if (count == numEvents - 1) {
                context.assertTrue(writtenNumbers.isEmpty());
                async.complete();
            }
        };
        Subscription sub = client.subscribe(descriptor, handler).get();
    }


    @Test
    //@Repeat(value = 10000)
    public void testSubscribeFromTimestamp(TestContext context) throws Exception {
        Producer prod = client.createProducer(TEST_CHANNEL_1);
        final int preEvents = 10;
        for (int i = 0; i < preEvents; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.publish(event);
            // wait for the 'ack' on the last event to ensure that it has be timestamped
            if (i == preEvents - 1) {
                cf.get();
            }
        }
        // take a local timestamp before adding more
        final long preTime = System.currentTimeMillis();

        // write some more after this timestamp
        final int postEvents = 10;
        for (int i = preEvents; i < postEvents + preEvents; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.publish(event);
            // wait for the 'ack' on the last event to ensure that it to has be timestamped
            if (i == preEvents + postEvents  - 1) {
                cf.get();
            }
        }

        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setStartTimestamp(preTime);

        Async async = context.async();
        AtomicLong lastPos = new AtomicLong(-1);
        AtomicInteger receivedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            long last = lastPos.get();
            context.assertTrue(re.channelPos() > last);
            lastPos.set(re.channelPos());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            // count should ignore the event previous to the timestamp
            context.assertEquals(count, (long)event.getInteger("num") - preEvents);
            context.assertTrue(re.timeStamp() >= preTime);
            if (count == postEvents - 1) {
                async.complete();
            }
        };
        Subscription sub = client.subscribe(descriptor, handler).get();
    }

    @Test
    public void testFailingSubsBuilder(TestContext context)  {

        final String filterName = "com.mewbase.filter.Not7";
        try { // no filter specified
            server.buildSubsFilter(TEST_CHANNEL_1)
                    .named(filterName)
                    .store();
        } catch (IllegalStateException ese) {
             assertTrue(ese.getMessage().contains("withFilter")); // leave a clue
        }

        try { // no name
            server.buildSubsFilter(TEST_CHANNEL_1)
                    .withFilter(event -> {
                        int val = event.getInteger("num");
                        return val != 7;
                    })
                    .store();
        } catch (IllegalStateException ese) {
            assertTrue(ese.getMessage().contains("named")); // leave a clue
        }
    }

    @Test
    //@Repeat(value = 10000)
    public void testSubscribeWithFilter(TestContext context) throws Exception {

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        final int events = 10;
        for (int i = 0; i < events; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.publish(event);
            if (i == events - 1) {
                cf.get();
            }
        }

        final String filterName = "com.mewbase.filter.Not7";
        server.buildSubsFilter(TEST_CHANNEL_1)
                .named(filterName)
                .withFilter(event -> {
                    int val = event.getInteger("num");
                    return val != 7;
                })
                .store();


        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setFilterName(filterName);
        descriptor.setStartEventNum(0);     // replay all the events and filter num 7

        Async async = context.async();

        AtomicInteger receivedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            // if the number is 7 the filter was not applied
            context.assertTrue((long)event.getInteger("num") != 7);
            if (count == events - 2) {
                async.complete();
            }
        };

        CompletableFuture<Subscription> fut = client.subscribe(descriptor, handler);
        Subscription sub = fut.get();
    }


    @Test
    public void testSubscribeWithWrongFilter(TestContext context) throws Exception {

        final TestAppender appender = patchTestAppender();

        Producer prod = client.createProducer(TEST_CHANNEL_1);
        final int events = 10;
        for (int i = 0; i < events; i++) {
            BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            CompletableFuture<Void> cf = prod.publish(event);
            if (i == events - 1) {
                cf.get();
            }
        }

        final String filterName = "com.mewbase.filter.Not7";
        final String wrongFilterName = "BadBadName";

        server.buildSubsFilter(TEST_CHANNEL_1)
                .named(filterName)
                .withFilter(event -> {
                    int val = event.getInteger("num");
                    return val != 7;
                })
                .store();


        SubDescriptor descriptor = new SubDescriptor();
        descriptor.setChannel(TEST_CHANNEL_1);
        descriptor.setFilterName(wrongFilterName);
        descriptor.setStartEventNum(0);     // replay all the events and filter num 7

        Async async = context.async();

        AtomicInteger receivedCount = new AtomicInteger();
        Consumer<ClientDelivery> handler = re -> {
            context.assertEquals(TEST_CHANNEL_1, re.channel());
            BsonObject event = re.event();
            long count = receivedCount.getAndIncrement();
            // should see all the even t
            if (count == events - 1) {
                async.complete();
            }
        };

        CompletableFuture<Subscription> fut = client.subscribe(descriptor, handler);
        Subscription sub = fut.get();

        List<LoggingEvent> logged = appender.getLog();
        // check that it reports the bad name
        assertTrue(logged.get(0).getMessage().toString().contains(wrongFilterName));
        // check that it does a match all in the case of failure
        assertTrue(logged.get(1).getMessage().toString().contains("match all"));
        removeTestAppender(appender);
    }

    
}
