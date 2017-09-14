package io.mewbase.binders.impl.lmdb;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderFactory;
import io.mewbase.binders.BinderStore;

import io.mewbase.server.MewbaseOptions;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;


public class LmdbBinderStore implements BinderStore {

    private final String DEFAULT_BINDER_OPTIONS = "Make this real";

    private final String BINDERS_BINDER_NAME = "_bbn";
    private Binder bindersBinder;

    private final ConcurrentMap<String, Binder> userBinders = new ConcurrentHashMap<>();
    private final BinderFactory binderFactory;


    public LmdbBinderStore(MewbaseOptions mewbaseOptions) {
        this(mewbaseOptions, Vertx.vertx());
    }

    public LmdbBinderStore(MewbaseOptions mewbaseOptions, Vertx vertx) {
        binderFactory = new LmdbBinderFactory(mewbaseOptions,vertx);
        binderFactory.start().thenCompose( v -> {
           bindersBinder = binderFactory.createBinder(BINDERS_BINDER_NAME);}
           ).thenCompose(v -> startUserBinders());

        })

    }

    @Override
    public CompletableFuture<Void> create(String name) {
        return null;
    }

    @Override
    public CompletableFuture<Binder> get(String name) {
        return null;
    }

    @Override
    public Stream<Binder> binders() {
        return userBinders.values().stream();
    }

    @Override
    public Stream<String> binderNames() {
        return userBinders.keySet().stream();
    }

    @Override
    public CompletableFuture<Void> delete(String name) {
        return null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return null;
    }




    private Binder loadBinder(String binderName) {
        Binder binder = binderFactory.createBinder(binderName);
        userbinders.put(binderName, binder);
        return binder;
   }

    private CompletableFuture<Void> startBinders(List<String> binderNames) {
        CompletableFuture[] arr = new CompletableFuture[binderNames.size()];
        int i = 0;
        for (String binderName : binderNames) {
            logger.trace("Starting binder: " + binderName);
            Binder binder = loadBinder(binderName);
            arr[i++] = binder.start();
        }
        return CompletableFuture.allOf(arr);
    }

//    private CompletableFuture<Void> startBinders() {
//        return systemBinderFactory.start().thenCompose(v -> startSystemBinders()).thenCompose(v -> startUserBinders());
//    }
//
//    private synchronized CompletableFuture<Void> stopBinders() {
//        CompletableFuture[] cfArr = new CompletableFuture[binders.size()];
//        int i = 0;
//        for (Binder binder : binders.values()) {
//            cfArr[i++] = binder.close();
//        }
//        CompletableFuture<Void> all = CompletableFuture.allOf(cfArr);
//        return all.thenCompose(v -> binderFactory.close());
//    }
//
//    private CompletableFuture<Void> insertBinder(String binderName) {
//
//        return bindersBinder.put(binderName, new BsonObject().put(Binder.ID_FIELD, binderName));
//    }
//
//    private CompletableFuture<Void> startSystemBinders() {
//        bindersBinder = loadBinder(BINDERS_BINDER_NAME);
//
//        return CompletableFuture.allOf(bindersBinder.start(), durableSubsBinder.start());
//    }
//
//    private Binder loadBinder(String binderName) {
//        Binder binder = systemBinderFactory.createBinder(binderName);
//        binders.put(binderName, binder);
//        return binder;
//    }
//
//    private CompletableFuture<Void> startBinders(List<String> binderNames) {
//        CompletableFuture[] arr = new CompletableFuture[binderNames.size()];
//        int i = 0;
//        for (String binderName : binderNames) {
//            logger.trace("Starting binder: " + binderName);
//            Binder binder = loadBinder(binderName);
//            arr[i++] = binder.start();
//        }
//        return CompletableFuture.allOf(arr);
//    }
//
//    private CompletableFuture<List<BsonObject>> listBinder(Binder binder) {
//        DocReadStream stream = binder.getMatching(doc -> true);
//        CompletableFuture<List<BsonObject>> cf = new CompletableFuture<>();
//        List<BsonObject> docs = new ArrayList<>();
//        if (stream.hasMore()) {
//            stream.handler(doc -> {
//                docs.add(doc);
//                if (!stream.hasMore()) {
//                    stream.close();
//                    cf.complete(docs);
//                }
//            });
//            stream.start();
//        } else {
//            cf.complete(docs);
//        }
//        return cf;
//    }
//
//    private CompletableFuture<Void> startUserBinders() {
//        CompletableFuture<List<BsonObject>> docsCf = listBinder(bindersBinder);
//        return docsCf.thenCompose(list -> {
//            List<String> ids = list.stream().map(doc -> doc.getString(Binder.ID_FIELD)).collect(Collectors.toList());
//            return startBinders(ids);
//        });
//    }
//



}
