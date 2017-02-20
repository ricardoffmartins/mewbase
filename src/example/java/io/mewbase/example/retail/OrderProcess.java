package io.mewbase.example.retail;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.client.ClientOptions;
import io.mewbase.client.MewException;
import io.mewbase.server.*;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

/**
 * Created by tim on 11/01/17.
 */
public class OrderProcess implements Mewblet {

    public static void main(String[] args) {
        try {
            // Setup and start a server - for a real standalone server it would automatically run any mewblets
            ServerOptions options = new ServerOptions();
            options.getNetServerOptions().setPort(7451);
            options.getHttpServerOptions().setPort(8080);
            Server server = Server.newServer(options);
            server.start().get();
            new OrderProcess().setup(server);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO expose the Mewbase vertx
    private Vertx vertx = Vertx.vertx();

    private HttpClient pickingServiceHttpClient;
    private HttpClient deliveryServiceHttpClient;


    @Override
    public void setup(Mewbase mewbase) throws Exception {


        // Order fulfillment process

        ClientOptions orderServiceOptions = new ClientOptions().setPort(7451);

        ProcessStageDefinition orderPlacedStage = mewbase.buildProcessStage("orderPlaced")
                .fromChannel("orders")
                .fromServer(orderServiceOptions)
                .filteredWith(ev -> ev.getString("eventType").equals("orderPlaced"))
                .identifiedBy(ev -> ev.getString("id"))
                .eventHandler((ev, ctx) -> {
                    System.out.println("Process received orderPlaced event");
                    ctx.getState().put("orderPlacedEvent", ev);
                    ctx.complete();
                })
                .thenDo(this::sendPickCommand)
                .build();

        ClientOptions pickingServiceOptions = new ClientOptions().setPort(7452);

        ProcessStageDefinition orderPickedStage = mewbase.buildProcessStage("orderPicked")
                .fromChannel("pickedOrders")
                .fromServer(pickingServiceOptions)
                .filteredWith(ev -> ev.getString("eventType").equals("orderPicked"))
                .identifiedBy(ev -> ev.getString("id"))
                .eventHandler((ev, ctx) -> {
                    System.out.println("Process received orderPicked event");
                    ctx.getState().put("orderPickedEvent", ev);
                    ctx.complete();
                })
                .thenDo(this::sendDeliverCommand)
                .build();

        ProcessDefinition processDefinition = mewbase.buildProcess("orderFulfillment")
                .addStage(orderPlacedStage)
                .addStage(orderPickedStage)
                .build();

        mewbase.registerProcess(processDefinition);

        HttpClientOptions pickingServiceHttpOptions = new HttpClientOptions().setDefaultPort(8081);
        pickingServiceHttpClient = vertx.createHttpClient(pickingServiceHttpOptions);

        HttpClientOptions deliveryServiceHttpOptions = new HttpClientOptions().setDefaultPort(8082);
        deliveryServiceHttpClient = vertx.createHttpClient(deliveryServiceHttpOptions);


        System.out.println("OrderService started");

    }

    private void sendPickCommand(StageContext stageContext) {
        // Send a pick order command
        BsonObject orderPlacedEvent = stageContext.getState().getBsonObject("orderPlacedEvent");
        String orderID = orderPlacedEvent.getString("id");
        // TODO parse params properly so don't have to add id to command
        sendHttpCommand(pickingServiceHttpClient, "/pickings/" + orderID + "/", new JsonObject().put("id", orderID), stageContext);
    }

    private void sendDeliverCommand(StageContext stageContext) {
        // Send a pick order command
        BsonObject orderPlacedEvent = stageContext.getState().getBsonObject("orderPlacedEvent");
        String orderID = orderPlacedEvent.getString("id");
        sendHttpCommand(deliveryServiceHttpClient, "/deliveries/" + orderID + "/", new JsonObject().put("id", orderID), stageContext);
    }

    private void sendHttpCommand(HttpClient client, String uri, JsonObject command, StageContext stageContext) {
        client.request(HttpMethod.POST, uri, resp -> {
            if (resp.statusCode() == 200) {
                stageContext.complete();
            } else {
                System.err.println("Received " + resp.statusCode() + " " + resp.statusMessage());
                stageContext.fail(new MewException("failed to send command"));
            }
        }).end(command.encode());

    }
}
