package io.zeebe.client.api.commands;

import io.zeebe.client.api.record.WorkflowInstanceRecord;

public interface WorkflowInstanceCommand extends WorkflowInstanceRecord
{

    WorkflowInstanceCommandName getName();

    enum WorkflowInstanceCommandName
    {
        CREATE_WORKFLOW_INSTANCE,
        CANCEL_WORKFLOW_INSTANCE,
        UPDATE_PAYLOAD,
    }
}
