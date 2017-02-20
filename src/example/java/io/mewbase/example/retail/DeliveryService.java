package io.mewbase.example.retail;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.*;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;

/**
 * Created by tim on 11/01/17.
 */
public class DeliveryService implements Mewblet {

    public static void main(String[] args) {
        try {
            // Setup and start a server - for a real standalone server it would automatically run any mewblets
            ServerOptions options = new ServerOptions();
            options.getNetServerOptions().setPort(7453);
            options.getHttpServerOptions().setPort(8082);
            Server server = Server.newServer(options);
            server.start().get();
            new DeliveryService().setup(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO expose the Mewbase vertx
    private Vertx vertx = Vertx.vertx();

    @Override
    public void setup(Mewbase mewbase) throws Exception {

        mewbase.createChannel("deliveredorders").get();

        mewbase.buildCommandHandler("deliverorder")
                .emittingTo("deliveredorders")
                .as((command, ctx) -> {
                    BsonObject event = new BsonObject().put("eventType", "orderDelivered");
                    CommandContext.putFields(command, event, "orderID");
                    ctx.publishEvent(event).complete();
                })
                .create();

        mewbase.exposeCommand("deliverorder", "/deliveries/:orderID/", HttpMethod.POST);

        System.out.println("DeliveryService started");

    }

}
