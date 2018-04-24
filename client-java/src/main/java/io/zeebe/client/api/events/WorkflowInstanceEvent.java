package io.zeebe.client.api.events;

import io.zeebe.client.api.record.WorkflowInstanceRecord;

public interface WorkflowInstanceEvent extends WorkflowInstanceRecord
{
    /**
     * @return the current state
     */
    WorkflowInstanceState getState();

    enum WorkflowInstanceState
    {
        CREATED,
        COMPLETED,
        CANCELED,

        PAYLOAD_UPDATED,

        START_EVENT_OCCURRED,
        END_EVENT_OCCURRED,

        SEQUENCE_FLOW_TAKEN,

        GATEWAY_ACTIVATED,

        ACTIVITY_READY,
        ACTIVITY_ACTIVATED,
        ACTIVITY_COMPLETING,
        ACTIVITY_COMPLETED,
        ACTIVITY_TERMINATE;
    }
}
