package io.mewbase.server.impl.cqrs;

import io.mewbase.bson.BsonObject;
import io.mewbase.server.CommandContext;
import io.mewbase.server.CommandHandler;
import io.mewbase.server.CommandHandlerBuilder;

import java.util.function.BiConsumer;

/**
 * Created by tim on 10/01/17.
 */
public class CommandHandlerBuilderImpl implements CommandHandlerBuilder {

    private final CQRSManager cqrsManager;
    private final CommandHandlerImpl commandHandler;

    public CommandHandlerBuilderImpl(CQRSManager cqrsManager, String commandName) {
        this.cqrsManager = cqrsManager;
        this.commandHandler = new CommandHandlerImpl(commandName);
    }

    @Override
    public CommandHandlerBuilder stringField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder integerField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder objectField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder mandatoryStringField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder mandatoryIntegerField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder mandatoryObjectField(String fieldName) {
        return null;
    }

    @Override
    public CommandHandlerBuilder as(BiConsumer<BsonObject, CommandContext> handler) {
        commandHandler.setHandler(handler);
        return this;
    }

    @Override
    public CommandHandlerBuilder emittingTo(String channelName) {
        commandHandler.setChannelName(channelName);
        return this;
    }

    @Override
    public CommandHandler create() {
        if (commandHandler.getChannelName() == null) {
            throw new IllegalStateException("Please specify a channel name");
        }
        if (commandHandler.getHandler() == null) {
            throw new IllegalStateException("Please specify a handler");
        }
        cqrsManager.registerCommandHandler(commandHandler);
        return commandHandler;
    }
}
