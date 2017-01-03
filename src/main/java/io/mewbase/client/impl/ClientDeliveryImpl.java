package io.mewbase.client.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.client.ClientDelivery;
import io.mewbase.client.Subscription;
import io.mewbase.common.impl.DeliveryImpl;

/**
 * Created by tim on 04/11/16.
 */
public class ClientDeliveryImpl extends DeliveryImpl implements ClientDelivery {

    protected final SubscriptionImpl sub;
    protected final int sizeBytes;

    public ClientDeliveryImpl(String channel, long timestamp, long sequenceNumber, BsonObject event, SubscriptionImpl sub, int sizeBytes) {
        super(channel, timestamp, sequenceNumber, event);
        this.sub = sub;
        this.sizeBytes = sizeBytes;
    }

    @Override
    public void acknowledge() {
        sub.acknowledge(channelPos, sizeBytes);
    }

    @Override
    public Subscription subscription() {
        return sub;
    }
}
