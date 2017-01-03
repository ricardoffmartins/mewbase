package io.mewbase.example.pubsub;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.mewbase.common.SubDescriptor;
import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;

/**
 * Created by tim on 08/11/16.
 */
public class PubSubExample {

    public static void main(String[] args) {
        try {
            new PubSubExample().example();
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Very simple example showing pub/sub messaging
     */
    private void example() throws Exception {

        // Setup and start a server
        ServerOptions options = new ServerOptions();
        Server server = Server.newServer(options);
        server.start().get();
        server.admin().createChannel("orders").get();

        // Create a client
        Client client = Client.newClient(new ClientOptions());

        // Subscribe to a channel
        SubDescriptor descriptor = new SubDescriptor().setChannel("orders");
        client.subscribe(descriptor, del -> {
            System.
                    out.println("Received event: " + del.event().getString("foo"));
        });

        // Publish to the channel
        client.publish("orders", new BsonObject().put("foo", "bar"));

        client.close().get();
        server.stop().get();
    }
}
