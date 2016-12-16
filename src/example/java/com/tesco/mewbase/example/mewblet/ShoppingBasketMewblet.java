package com.tesco.mewbase.example.mewblet;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.bson.BsonPath;
import com.tesco.mewbase.client.Client;
import com.tesco.mewbase.client.ClientOptions;
import com.tesco.mewbase.server.MewAdmin;
import com.tesco.mewbase.server.Mewblet;
import com.tesco.mewbase.server.Server;
import com.tesco.mewbase.server.ServerOptions;

/**
 * Created by tim on 08/11/16.
 */
public class ShoppingBasketMewblet implements Mewblet {

    public static void main(String[] args) {
        try {
            new ShoppingBasketMewblet().example();
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    Simple shopping basket example
     */
    private void example() throws Exception {

        // Setup and start a server - for a real standalone server it would automatically run any mewblets
        ServerOptions options =
                new ServerOptions().setChannels(new String[]{"orders"}).setBinders(new String[]{"baskets"});
        Server server = Server.newServer(options);
        server.start().get();
        setup(server.admin());

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
    }

    @Override
    public void setup(MewAdmin admin) {

        admin.createChannel("orders");
        admin.createBinder("baskets");

        admin.buildProjection("maintain_basket")                             // projection name
                .projecting("orders")                                           // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("add_item")) // event filter
                .onto("baskets")                                                // binder name
                .identifiedBy(ev -> ev.getString("basketID"))                   // document id selector; how to obtain the doc id from the event bson
                .as((basket, del) ->                                            // projection function
                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID")))
                .create();

    }
}
