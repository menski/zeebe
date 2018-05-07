package io.zeebe.broker.system.deployment.data;

import static io.zeebe.logstreams.log.LogStream.MAX_TOPIC_NAME_LENGTH;

import io.zeebe.broker.clustering.orchestration.topic.TopicState;
import io.zeebe.map.Bytes2LongZbMap;
import org.agrona.DirectBuffer;

/**
 * Set of defined topics: when a deployment CREATE command is processed,
 * this set is used to verify whether the referenced topic exists.
 *<p>
 * Note that this set is populated based on the {@link TopicState#CREATING} state.
 * Why? Once a topic is in state CREATING, it should be possible to deploy workflows
 * for this topic.
 */
public class TopicNames
{
    private static final long EXISTS_VALUE = 1;
    private static final long NOT_EXISTS_VALUE = -1;

    private final Bytes2LongZbMap map = new Bytes2LongZbMap(MAX_TOPIC_NAME_LENGTH);

    public void addTopic(DirectBuffer name)
    {
        map.put(name, 0, name.capacity(), EXISTS_VALUE);
    }

    public boolean exists(DirectBuffer topicName)
    {
        return map.get(topicName, 0, topicName.capacity(), NOT_EXISTS_VALUE) == EXISTS_VALUE;
    }

    public Bytes2LongZbMap getRawMap()
    {
        return map;
    }
}
