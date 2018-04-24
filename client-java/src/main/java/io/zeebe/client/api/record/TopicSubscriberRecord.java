package io.zeebe.client.api.record;

public interface TopicSubscriberRecord extends Record
{
    /**
     * @return the name of the subscription
     */
    String getName();

    int getPrefetchCapacity();

    long getStartPosition();

    boolean isForceStart();

}
