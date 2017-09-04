package io.mewbase.example.mewblet;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;

/**
 * Created by tim on 08/11/16.
 */
public class ShoppingBasketExample {

    public static void main(String[] args) {
        try {
            new ShoppingBasketExample().example();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Simple shopping basket example
     */
    private void example() throws Exception {

        // Setup and start a server - for a real standalone server it would automatically run any mewblets
        ServerOptions options = new ServerOptions();
        Server server = Server.newServer(options);
        server.start().get();
        new ShoppingBasketMewblet().setup(server);

        // Create a client
        Client client = Client.newClient(new ClientOptions());

        // Send some add/remove events

        BsonObject event = new BsonObject().put("eventType", "add_item").put("basketID", "basket1111");

        // need to connect to 
        client.publish("orders", event.copy().put("productID", "prod1234").put("quantity", 2));
        client.publish("orders", event.copy().put("productID", "prod2341").put("quantity", 1));
        client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", 3));
        client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", -1));

        Thread.sleep(1000);

        // Now get the basket
        BsonObject basket = client.findByID("baskets", "basket1111").get();

        System.out.println("Basket is: " + basket);

        client.close().get();
        server.stop().get();
    }

}
