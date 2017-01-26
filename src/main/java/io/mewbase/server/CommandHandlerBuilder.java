package io.mewbase.server;

import io.mewbase.bson.BsonObject;
import io.mewbase.common.Delivery;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by tim on 07/01/17.
 */
public interface CommandHandlerBuilder {


    CommandHandlerBuilder stringField(String fieldName);

    CommandHandlerBuilder integerField(String fieldName);

    CommandHandlerBuilder objectField(String fieldName);

    CommandHandlerBuilder mandatoryStringField(String fieldName);

    CommandHandlerBuilder mandatoryIntegerField(String fieldName);

    CommandHandlerBuilder mandatoryObjectField(String fieldName);

    CommandHandlerBuilder as(BiConsumer<BsonObject, CommandContext> commandHandler);

    CommandHandlerBuilder emittingTo(String channelName);

    CommandHandler create();
}
