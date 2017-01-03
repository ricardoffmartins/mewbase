package io.mewbase.client;

import io.mewbase.common.Delivery;

/**
 * Created by tim on 04/11/16.
 */
public interface ClientDelivery extends Delivery {

    void acknowledge();

    Subscription subscription();
}
