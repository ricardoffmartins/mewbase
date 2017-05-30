package io.mewbase.example.shopping;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.bson.Path;
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
        System.out.println("Main complete");
    }

    /*
    Simple shopping basket example
     */
    private void example() throws Exception {

        // Setup and start a server
        ServerOptions options = new ServerOptions();
        Server server = Server.newServer(options);
        server.start().get();
        server.createBinder("baskets").get();
        server.createChannel("orders").get();

        // Register a projection that will respond to add_item events and increase/decrease the quantity of the item in the basket
        server.buildProjection("maintain_basket")                             // projection name
                .projecting("orders")                                           // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("add_item")) // event filter
                .onto("baskets")                                                // binder name
                .identifiedBy(ev -> ev.getString("basketID"))                   // document id selector; how to obtain the doc id from the event bson
                .as((basket, del) -> {                                          // projection function
                    final Path path = new Path("products." + del.event().getString("productID"));
                    return BsonPath.add(basket, path, del.event().getInteger("quantity"));
                } )

                .create();

        // Create a client
        Client client = Client.newClient(new ClientOptions());

        // Send some add/remove events

        BsonObject event = new BsonObject().put("eventType", "add_item").put("basketID", "basket1111");

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
