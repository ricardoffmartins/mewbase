package io.mewbase.example.shopping;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;
import io.mewbase.client.Client;
import io.mewbase.client.ClientOptions;
import io.mewbase.common.Delivery;
import io.mewbase.server.Server;
import io.mewbase.server.ServerOptions;


/**
 * Created by Nige on 17/05/17.
 */
public class FridgeExample {

    public static void main(String[] args) {
        try {
            new FridgeExample().exampleServer();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Main complete");
    }

    /**
    Simple Fridge example
     */
    private void exampleServer() throws Exception {

        // Setup and start a server
        Server server = Server.newServer(new ServerOptions());
        server.start().get();
        server.createChannel("fridge.status").get();
        server.createBinder("fridges").get();

        // Register a projection that will respond to fridge door status events
        server.buildProjection("maintain_fridge_status")              // projection name
                .projecting("fridge.status")                                         // channel name
                .filteredBy(ev -> ev.getString("eventType").equals("doorStatus"))     // event filter
                .onto("fridges")                                                     // binder name
                .identifiedBy(ev -> ev.getString("fridgeID"))                   // document id selector; how to obtain the doc id from the event bson
                .as( (BsonObject fridge, Delivery del) ->  { // projection function
                        BsonObject evt = del.event();
                        long time =  del.timeStamp();
                        String doorStatus = evt.getString("status");
                        BsonPath.set(fridge, time, "timeStamp");
                        BsonPath.set(fridge, doorStatus, "door");
                        return fridge;
                } )
                .create();

        // run the client to exercise the server
        exampleClient();
        server.stop().get();
    }


    private void exampleClient() throws Exception {

        // Create a client
        Client client = Client.newClient(new ClientOptions());

        // Send some open close events for this fridge
        BsonObject event = new BsonObject().put("fridgeID", "f1").put("eventType", "doorStatus");

        // Open the door
        client.publish("fridge.status", event.copy().put("status", "open"));
        // wait for event to log and projection to fire
        Thread.sleep(100);

        // Now get the status
        BsonObject fridge = client.findByID("fridges", "f1").get();
        System.out.println("Fridge is: " + fridge);

        // Shut the door
        client.publish("fridge.status", event.copy().put("status", "shut"));
        Thread.sleep(100);

        // Now get the fridge details again
        fridge = client.findByID("fridges", "f1").get();
        System.out.println("Fridge is: " + fridge);

        client.close().get();
    }

}
