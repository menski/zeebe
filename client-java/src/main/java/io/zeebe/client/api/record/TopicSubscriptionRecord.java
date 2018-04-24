package io.zeebe.client.api.record;

public interface TopicSubscriptionRecord extends Record
{
    /**
     * @return the name of the subscription
     */
    String getName();

    long getAckPosition();

}
