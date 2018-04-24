package io.zeebe.client.api.events;

import io.zeebe.client.api.record.TopicRecord;

public interface TopicEvent extends TopicRecord
{
    /**
     * @return the current state
     */
    TopicState getState();

    enum TopicState
    {
        CREATING
    }
}
