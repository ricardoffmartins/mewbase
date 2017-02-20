package io.mewbase.example.retail;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.server.*;
import io.vertx.core.http.HttpMethod;

import java.util.UUID;

/**
 * Created by tim on 11/01/17.
 */
public class OrderService implements Mewblet {

    public static void main(String[] args) {
        try {
            // Setup and start a server - for a real standalone server it would automatically run any mewblets
            ServerOptions options = new ServerOptions();
            options.getNetServerOptions().setPort(7451);
            options.getHttpServerOptions().setPort(8080);
            Server server = Server.newServer(options);
            server.start().get();
            new OrderService().setup(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setup(Mewbase mewbase) throws Exception {

        mewbase.createChannel("orders").get();
        mewbase.createBinder("baskets").get();

        mewbase.buildProjection("maintain_basket")                     // projection name
                .projecting("orders")                                                // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("addItem"))  // event filter
                .onto("baskets")                                                     // binder name
                .identifiedBy(ev -> ev.getString("customerID"))                 // document id selector; how to obtain the doc id from the event bson
                .as((basket, del) -> {                                               // projection function
                    System.out.println("In projection");
                    return BsonPath.add(basket, del.event().getInteger("quantity"), "products",
                            del.event().getString("productID"));
                })
                .create();

        mewbase.buildCommandHandler("addItem")
                .emittingTo("orders")
                .as((command, ctx) -> {
                    System.out.println("In addItem command handler");
                    BsonObject event = new BsonObject().put("eventType", "addItem");
                    CommandContext.putFields(command, event, "customerID", "productID", "quantity");
                    System.out.println("Publishing event " + event);
                    ctx.publishEvent(event).complete();
                })
                .create();

        mewbase.buildCommandHandler("placeOrder")
                .emittingTo("orders")
                .as((command, ctx) -> {
                    BsonObject event = new BsonObject().put("eventType", "orderPlaced");
                    CommandContext.putFields(command, event, "customerID");
                    // Retrieve the basket and add it to the event
                    mewbase.getBinder("baskets").get("customerID").whenComplete((basket, t) -> {
                        if (t == null) {
                            String orderID = UUID.randomUUID().toString();
                            event.put("id", orderID);
                            event.put("order", basket);
                            System.out.println("Published order placed event");
                            ctx.publishEvent(event).complete();
                        } else {
                            // TODO fail context
                        }
                    });
                })
                .create();

        mewbase.buildQuery("allBaskets")
                .from("baskets")
                .documentFilter((doc, ctx) -> true)
                .create();

        mewbase
                .exposeCommand("addItem", "/baskets/:customerID/", HttpMethod.PATCH)
                .exposeCommand("placeOrder", "/orders/:customerID/", HttpMethod.POST)
                .exposeQuery("allBaskets", "/baskets/")
                .exposeFindByID("baskets", "/baskets/:id/");

        System.out.println("OrderService started");

    }

}
