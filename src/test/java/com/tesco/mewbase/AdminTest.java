package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonArray;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 26/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class AdminTest extends ServerTestBase {

    private final static Logger logger = LoggerFactory.getLogger(AdminTest.class);

    @Test
    public void testListBinders() throws Exception {
        int numBinders = 10;
        CompletableFuture[] all = new CompletableFuture[numBinders];
        for (int i = 0; i < numBinders; i++) {
            all[i] = server.admin().createBinder("testbinder" + i);
        }
        CompletableFuture.allOf(all).get();
        BsonArray binders1 = client.listBinders().get();

        Set<String> bindersSet1 = new HashSet<>(binders1.getList());
        for (int i = 0; i < numBinders; i++) {
            assertTrue(bindersSet1.contains("testbinder" + i));
        }

        final String otherBinderName = "someotherbinder";

        // Create a new one
        server.admin().createBinder(otherBinderName).get();
        BsonArray binders2 = client.listBinders().get();
        Set<String> bindersSet2 = new HashSet<>(binders2.getList());
        assertTrue(bindersSet2.contains(otherBinderName));
        assertEquals(bindersSet1.size() + 1, bindersSet2.size());
    }

    @Test
    public void testCreateBinder() throws Exception {
        final String binderName = "somebinder";
        CompletableFuture<Boolean> cf = client.createBinder(binderName);
        assertTrue(cf.get());

        List<String> binderNames = server.admin().listBinders();
        Set<String> bindersSet = new HashSet<>(binderNames);
        assertTrue(bindersSet.contains(binderName));

        CompletableFuture<Boolean> cf2 = client.createBinder(binderName);
        assertFalse(cf2.get());
    }

    @Test
    public void testListChannels() throws Exception {
        int numChannels = 10;
        CompletableFuture[] all = new CompletableFuture[numChannels];
        for (int i = 0; i < numChannels; i++) {
            all[i] = server.admin().createChannel("testchannel" + i);
        }
        CompletableFuture.allOf(all).get();
        BsonArray channels1 = client.listChannels().get();

        Set<String> channelsSet1 = new HashSet<>(channels1.getList());
        for (int i = 0; i < numChannels; i++) {
            assertTrue(channelsSet1.contains("testchannel" + i));
        }

        final String otherChannelName = "someotherchannel";

        // Create a new one
        server.admin().createChannel(otherChannelName).get();
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

        List<String> channelNames = server.admin().listChannels();
        Set<String> channelsSet = new HashSet<>(channelNames);
        assertTrue(channelsSet.contains(channelName));

        CompletableFuture<Boolean> cf2 = client.createChannel(channelName);
        assertFalse(cf2.get());
    }


}
