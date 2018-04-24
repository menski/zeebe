package io.zeebe.client.api.record;

public interface TopicRecord extends Record
{
    /**
     * @return the name of the subscription
     */
    String getName();

    int getPartitions();

    int getReplicationFactor();

}
