package io.mewbase.example.retail;

import io.mewbase.bson.BsonPath;
import io.mewbase.server.Mewbase;
import io.mewbase.server.Mewblet;
import io.mewbase.server.RESTServiceAdaptor;
import io.mewbase.server.impl.RESTServiceAdaptorImpl;
import io.vertx.core.http.HttpMethod;

/**
 * Created by tim on 11/01/17.
 */
public class OrderServiceMewblet implements Mewblet {

    @Override
    public void setup(Mewbase mewbase) throws Exception {

        mewbase.createChannel("orders").get();
        mewbase.createBinder("baskets").get();

        mewbase.buildProjection("maintain_basket")                                // projection name
                .projecting("orders")                                           // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("add_item")) // event filter
                .onto("baskets")                                                // binder name
                .identifiedBy(ev -> ev.getString("basketID"))                   // document id selector; how to obtain the doc id from the event bson
                .as((basket, del) ->                                            // projection function
                        BsonPath.add(basket, del.event().getInteger("quantity"), "products", del.event().getString("productID")))
                .create();

        mewbase.buildCommandHandler("addItem")
                .emittingTo("orders")
                .as((command, ctx) -> {
                    ctx.publishEvent(ctx.putFields(command, "customerID", "productID", "quantity")).complete();
                })
                .create();

        mewbase.buildCommandHandler("placeOrder")
                .emittingTo("orders")
                .as((command, ctx) -> {
                    // TODO
                })
                .create();

        mewbase.buildQuery("allBaskets")
                .from("baskets")
                .documentFilter((doc, ctx) -> true)
                .create();

        RESTServiceAdaptor restServiceAdaptor = new RESTServiceAdaptorImpl(null);

        restServiceAdaptor
                .exposeCommand("addItem", "/baskets/:customerID/", HttpMethod.PATCH)
                .exposeCommand("placeOrder", "/orders/:customerID/", HttpMethod.POST)
                .exposeQuery("allBaskets", "/baskets/")
                .exposeFindByID("baskets", "/baskets/:customerID/");


    }
}
