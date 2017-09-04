package io.mewbase.server.impl.cqrs;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.CommandContext;
import io.mewbase.server.CommandHandler;

import java.util.function.BiConsumer;

/**
 * Created by tim on 10/01/17.
 */
public class CommandHandlerImpl implements CommandHandler {

    private final String name;
    private String channelName;
    private BiConsumer<BsonObject, CommandContext> handler;
    private Log log;

    public CommandHandlerImpl(String name) {
        this.name = name;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public BiConsumer<BsonObject, CommandContext> getHandler() {
        return handler;
    }

    public void setHandler(BiConsumer<BsonObject, CommandContext> handler) {
        this.handler = handler;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void pause() {
    }

    @Override
    public void resume() {

    }

    public Log getLog() {
        return log;
    }

    public void setLog(Log log) {
        this.log = log;
    }
}
