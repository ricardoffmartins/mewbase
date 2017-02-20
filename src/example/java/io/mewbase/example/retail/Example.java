package io.mewbase.example.retail;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;

/**
 * Created by tim on 01/02/17.
 */
public class Example {

    public static void main(String[] args) {
        try {
            Example ex = new Example();
            ex.startServers();
            ex.restExample();
            Thread.sleep(Long.MAX_VALUE);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void startServers() {
        DeliveryService.main(new String[] {});
        PickingService.main(new String[] {});
        OrderService.main(new String[]{});
    }

    private void restExample() throws Exception {

        Vertx vertx = Vertx.vertx();

        HttpClientOptions options = new HttpClientOptions().setDefaultPort(8080);
        HttpClient client = vertx.createHttpClient(options);

        // Add some items to the basket

        String customerID = "customer12345";

        sendAddItem(customerID, "product123", 1, client);

        Thread.sleep(1000);

        // Now view the basket

        client.getNow("/baskets/" + customerID + "/", resp -> {
            if (resp.statusCode() == 200) {
                System.out.println("Got response");
                resp.bodyHandler(buff -> {
                    System.out.println("Basket is: " + buff);
                });
            } else {
                System.err.println("Received " + resp.statusCode() + " " + resp.statusMessage());
            }
        });

        Thread.sleep(1000);

        // Now place an order

        client.post("/orders/" + customerID + "/", resp -> {
            if (resp.statusCode() == 200) {
                System.out.println("Order placed ok");
            } else {
                System.err.println("Received " + resp.statusCode() + " " + resp.statusMessage());
            }
        }).end();

        Thread.sleep(1000);

    }

    private void sendAddItem(String customerID, String productID, int quantity, HttpClient client) {
        JsonObject command = new JsonObject().put("productID", productID).put("quantity", quantity).put("customerID", customerID);
        client.request(HttpMethod.PATCH, "/baskets/" + customerID + "/", resp -> {
            if (resp.statusCode() == 200) {
                System.out.println("Add item sent ok");
            } else {
                System.err.println("Received " + resp.statusCode() + " " + resp.statusMessage());
            }
        }).end(command.encode());
    }
}
