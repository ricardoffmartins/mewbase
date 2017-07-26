package io.mewbase.example.client;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.Producer;

import java.util.concurrent.CompletableFuture;

/**
 * This is an example of a client that can be used with a running server.
 *
 * To get a server running on your local machine see the README.MD
 *
 * Once a server is running - we assume on localhost and on the default port -
 * this client will then write a set of events to the server.
 *
 */
public class StandaloneClient {

    private static final int EVENTS = 1000;


    public static void main(String[] args) {
        try {

            new StandaloneClient().run();
            System.out.println(EVENTS + " events published to server.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static final String HOST = "localhost"; // ClientOptions.DEFAULT_HOST;
    private static final int PORT = 7451; // ClientOptions.DEFAULT_PORT;

    private static final String CHANNEL = "Channel1";

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
        for (int  i = 0;  i < EVENTS ; ++i) {
            final CompletableFuture<Void> published = producer.publish(new BsonObject().put("foo", "bar").put("num", i));
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
