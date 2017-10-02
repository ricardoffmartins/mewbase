package io.mewbase.example.shopping;

import io.mewbase.bson.BsonObject;
import io.mewbase.bson.BsonPath;

import io.mewbase.server.Server;
import io.mewbase.server.MewbaseOptions;


/**
 * Fridge Example
 *
 * We assume that we have a number of Fridges (Refrigerators) that can raise IOT style events.
 * Each time the Fridge produces an event we Publish the event via the mewbase client to the server.
 * For each Fridge we maintain a Document in the 'fridges' Binder that reflects the current state
 * of each of the fridges that are sending out events.
 *
 * Some extension that may be informative  ...
 *
 * 1) Introduce events from fridges with new Ids and check these also appear the Binder.
 * 2) Introduce a new event that reflects the temperature of the fridge and integrate this into
 * the current projection, or make a new projection to handle the new status event.
 *
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
        final Server server = Server.newServer(new MewbaseOptions());
        server.start().get();

        // server.createBinder("fridges").get();

        // Register a projection that will respond to fridge door status events
//        server.buildProjection("maintain_fridge_status")              // projection name
//                .projecting("fridge.status")                                         // channel name
//                .filteredBy(ev -> ev.getString("eventType").equals("doorStatus"))     // event filter
//                .onto("fridges")                                                     // binder name
//                .identifiedBy(ev -> ev.getString("fridgeID"))                   // document id selector; how to obtain the doc id from the event bson
//                .as( (BsonObject fridge, BsonObject event) ->  { // projection function
//
//                        final String doorStatus = event.getString("status");
//                        BsonPath.set(fridge, doorStatus, "door");
//                        return fridge;
//                } )
//                .create();

        // run the client to exercise the server
        exampleClient();
        server.stop().get();
    }


    private void exampleClient() throws Exception {

        // Create a client
        //final Client client = Client.newClient(new ClientOptions());

        // Send some open close events for this fridge
        BsonObject event = new BsonObject().put("fridgeID", "f1").put("eventType", "doorStatus");

        // Open the door
        // TODO -
        // client.publish("fridge.status", event.copy().put("status", "open"));
        // wait for event to log and projection to fire
        Thread.sleep(100);

        // Now get the status
       // BsonObject fridgeState1 = client.findByID("fridges", "f1").get();
       // System.out.println("Fridge State is :" + fridgeState1);

        // Shut the door
        // TODO
        // client.publish("fridge.status", event.copy().put("status", "shut"));
        Thread.sleep(100);

        // Now get the fridge state again
        //BsonObject fridgeState2 = client.findByID("fridges", "f1").get();
       //  System.out.println("Fridge State is :" + fridgeState2);

        // client.close().get();
    }

}
