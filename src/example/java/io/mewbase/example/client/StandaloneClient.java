package io.mewbase.example.client;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * This is an example of a Client that can be used with a Mewbase server running
 * in a separate JVM; either on the same machine or over the network.
 *
 * To get a server running on your local or remote machine see the project
 * level README.MD
 *
 * Once a server is running - we assume on localhost and on the default port -
 * this client will then write a set of events to the server.
 *
 * Look to the HOST and PORT values if you run remotely and/or on another port.
 *
 * Change the EVENT_COUNT to the number of events that you want to
 */
public class StandaloneClient {


    private static final String HOST = "localhost"; // also as  ClientOptions.DEFAULT_HOST;
    private static final int PORT = 7451; // also as ClientOptions.DEFAULT_PORT;

    private static final int EVENT_COUNT = 1000;

    private static final String CHANNEL = "Channel1";


    public static void main(String[] args) {
        try {
            new StandaloneClient().run();
            System.out.println(EVENT_COUNT + " events published to server.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() throws Exception {
        ClientOptions opts = new ClientOptions().
                setHost(HOST).
                setPort(PORT);

        // Create a client with the given options
        Client client = Client.newClient(opts);

        // now create a channel to write to and wait for the channel to be constructed in the server
        client.createChannel(CHANNEL).get();

        // and use a producer to write to the channel
        Producer producer = client.createProducer(CHANNEL);

        // write EVENTS number of events to the Channel.
        for (int  i = 0;  i < EVENT_COUNT ; ++i) {
            final BsonObject event = new BsonObject().put("foo", "bar").put("num", i);
            final CompletableFuture<Void> published = producer.publish(event);
            Thread.sleep(10);
            published.whenComplete( (good,bad) -> {
               if (bad != null) {
                   bad.printStackTrace();
               }
            });
        }
        client.close();
    }

}
