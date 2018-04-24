package io.zeebe.client.api.commands;

public interface PartitionInfo
{
    /**
     * @return the partition's id
     */
    int getPartitionId();

    /**
     * @return the name of the topic this partition belongs to
     */
    String getTopicName();

    /**
     * @return the current role of the broker for this partition (i.e. leader or
     *         follower)
     */
    PartitionBrokerRole getRole();

    /**
     * @return <code>true</code> if the broker is the current leader of this
     *         partition
     */
    boolean isLeader();

    enum PartitionBrokerRole
    {
        LEADER, FOLLOWER
    }

}
