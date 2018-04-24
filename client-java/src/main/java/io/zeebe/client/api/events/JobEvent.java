package io.zeebe.client.api.events;

import io.zeebe.client.api.record.JobRecord;

public interface JobEvent extends JobRecord
{
    /**
     * @return the current state
     */
    JobState getState();

    // TODO: consider separate enums; eclipse autocomplete is not really happy with nested enums by defualt

    enum JobState
    {
        CREATED,
        LOCKED,
        COMPLETED,
        LOCK_EXPIRED,
        FAILED,
        RETRIES_UPDATED,
        CANCELED,
    }
}
