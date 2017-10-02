package io.mewbase.server.impl.cqrs;

import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.lmdb.LmdbBinderStore;
import io.mewbase.bson.BsonObject;

import io.mewbase.binders.Binder;
import io.mewbase.server.CommandContext;
import io.mewbase.server.CommandHandlerBuilder;
import io.mewbase.server.impl.ServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by tim on 10/01/17.
 */
public class CQRSManager {

    private final static Logger logger = LoggerFactory.getLogger(CQRSManager.class);

    private final ServerImpl server;

    private final BinderStore store = new LmdbBinderStore();
    private final Map<String, CommandHandlerImpl> commandHandlers = new ConcurrentHashMap<>();
    private final Map<String, QueryImpl> queries = new ConcurrentHashMap<>();

    public CQRSManager(ServerImpl server) {
        this.server = server;
    }

    public CommandHandlerBuilder buildCommandHandler(String commandName) {
        return new CommandHandlerBuilderImpl(this, commandName);
    }

    synchronized void registerCommandHandler(CommandHandlerImpl commandHandler) {
        if (commandHandlers.containsKey(commandHandler.getName())) {
            throw new IllegalArgumentException("Command handler " + commandHandler.getName() + " already registered");
        }

        // TODO - Use new EventSource
       // Log log = server.getLog(commandHandler.getChannelName());
//        if (log == null) {
//            throw new IllegalArgumentException("No such channel" + commandHandler.getChannelName());
//        }
       // commandHandler.setLog(log);
        commandHandlers.put(commandHandler.getName(), commandHandler);
    }

    public CompletableFuture<Void> callCommandHandler(String commandName, BsonObject commandBson) {

        CompletableFuture<Void> cfCommand = new CompletableFuture<>();

        CommandHandlerImpl commandHandler = commandHandlers.get(commandName);
        if (commandHandler == null) {
            String msg = "No handler for " + commandName;
            logger.trace(msg);
            cfCommand.completeExceptionally(new Exception(msg));
        } else {

            // TODO do command handlers need idempotency built in?

            // 1. TODO verify fields of command

            // 2. call handler

            CommandContextImpl context = new CommandContextImpl();

            context.whenComplete((v, t) -> {
                if (t != null) {
                    cfCommand.completeExceptionally(t);
                } else {
                    // 3. Actually publish the events
                    // TODO this should be in a transaction
                    CompletableFuture[] cfArr = new CompletableFuture[context.eventsToPublish.size()];
                    int i = 0;
                    // TDOD - EventSource
//                    for (BsonObject event : context.eventsToPublish) {
//                        CompletableFuture<Long> cfPub = server.publishEvent(commandHandler.getLog(), event);
//                        cfArr[i++] = cfPub;
//                    }
                    CompletableFuture<Void> all = CompletableFuture.allOf(cfArr);
                    all.whenComplete((v2, t2) -> {
                        if (t2 != null) {
                            cfCommand.completeExceptionally(t2);
                        } else {
                            cfCommand.complete(null);
                        }
                    });
                }
            });

            try {
                commandHandler.getHandler().accept(commandBson, context);
            } catch (Throwable t) {
                logger.warn("Failure in command handler", t);
                cfCommand.completeExceptionally(t);
            }
        }

        return cfCommand;
    }

    class CommandContextImpl extends CommandContext {

        List<BsonObject> eventsToPublish = new ArrayList<>();

        @Override
        public CommandContext publishEvent(BsonObject event) {
            eventsToPublish.add(event);
            return this;
        }

    }

    synchronized void registerQuery(QueryImpl query) throws ExecutionException, InterruptedException {
        if (queries.containsKey(query.getName())) {
            throw new IllegalArgumentException("Query " + query.getName() + " already registered");
        }
        Binder binder = store.open(query.getBinderName()).get();
        if (binder == null) {
            throw new IllegalArgumentException("No such binder " + query.getBinderName());
        }
        query.setBinder(binder);
        queries.put(query.getName(), query);
    }

    public QueryImpl getQuery(String queryName) {
        return queries.get(queryName);
    }

}
