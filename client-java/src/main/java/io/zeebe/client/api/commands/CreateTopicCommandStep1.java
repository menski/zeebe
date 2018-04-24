package io.zeebe.client.api.commands;

import io.zeebe.client.api.events.TopicEvent;

public interface CreateTopicCommandStep1
{
    /**
     * Set the name of the topic to create to. The name must be unique within
     * the broker/cluster.
     *
     * @param topicName
     *            the unique name of the new topic
     *
     * @return the builder for this command
     */
    CreateTopicCommandStep2 name(String topicName);

    interface CreateTopicCommandStep2
    {
        /**
         * Set the number of partitions to create for this topic.
         *
         * @param partitions
         *            the number of partitions for this topic
         *
         * @return the builder for this command.
         */
        CreateTopicCommandStep3 partitions(int partitions);
    }

    interface CreateTopicCommandStep3
    {
        /**
         * Set the replication factor of this topic (i.e. the number of replications of a partition).
         *
         * @param replicationFactor
         *            the replication factor of this topic
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateTopicCommandStep4 replicationFactor(int replicationFactor);
    }

    interface CreateTopicCommandStep4 extends FinalCommandStep<TopicEvent>
    {
        // the place for new optional parameters
    }

}
