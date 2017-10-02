package io.mewbase.example.shopping;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;

import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;

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
        MewbaseOptions options = new MewbaseOptions();
        Server server = Server.newServer(options);
        server.start().get();

//        server.createBinder("baskets").get();
//
//        // TODO - replace when we work out how to do this with the abstracted channels
//        // server.createChannel("orders").get();
//
//        // Register a projection that will respond to add_item events and increase/decrease the quantity of the item in the basket
//        server.buildProjection("maintain_basket")                             // projection name
//                .projecting("orders")                                           // channel name
//                .filteredBy(ev -> ev.getString("eventType").equals("add_item")) // event filter
//                .onto("baskets")                                                // binder name
//                .identifiedBy(ev -> ev.getString("basketID"))                   // document id selector; how to obtain the doc id from the event bson
//                .as((basket, event) -> // projection function
//                        BsonPath.add(basket, event.getInteger("quantity"), "products", event.getString("productID")))
//                .create();

        // Create a client
        //Client client = Client.newClient(new ClientOptions());

        // Send some add/remove events

        BsonObject event = new BsonObject().put("eventType", "add_item").put("basketID", "basket1111");

        // TODO with new Streaming Server
       // client.publish("orders", event.copy().put("productID", "prod1234").put("quantity", 2));
       // client.publish("orders", event.copy().put("productID", "prod2341").put("quantity", 1));
       // client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", 3));
       // client.publish("orders", event.copy().put("productID", "prod5432").put("quantity", -1));

        Thread.sleep(1000);

        // Now get the basket
       // BsonObject basket = client.findByID("baskets", "basket1111").get();

       // System.out.println("Basket is: " + basket);

       // client.close().get();
        server.stop().get();
    }
}
