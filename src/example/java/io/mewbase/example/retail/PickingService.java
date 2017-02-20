package io.mewbase.example.retail;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.client.ClientOptions;
import io.mewbase.server.*;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/01/17.
 */
public class PickingService implements Mewblet {

    public static void main(String[] args) {
        try {
            // Setup and start a server - for a real standalone server it would automatically run any mewblets
            ServerOptions options = new ServerOptions();
            options.getNetServerOptions().setPort(7452);
            options.getHttpServerOptions().setPort(8081);
            Server server = Server.newServer(options);
            server.start().get();
            new PickingService().setup(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO expose the Mewbase vertx
    private Vertx vertx = Vertx.vertx();

    @Override
    public void setup(Mewbase mewbase) throws Exception {

        mewbase.createChannel("pickedOrders").get();

        mewbase.buildCommandHandler("pickOrder")
                .emittingTo("pickedOrders")
                .as((command, ctx) -> {
                    System.out.println("Received pickOrder command");
                    BsonObject event = new BsonObject().put("eventType", "orderPicked");
                    CommandContext.putFields(command, event, "id");
                    ctx.publishEvent(event).complete();
                })
                .create();

        mewbase.exposeCommand("pickOrder", "/pickings/:id/", HttpMethod.POST);

        System.out.println("PickingService started");

    }

}
