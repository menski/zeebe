package io.zeebe.client.api.commands;

import java.io.InputStream;

import io.zeebe.client.api.events.WorkflowInstanceEvent;

public interface UpdatePayloadWorkflowInstanceCommandStep1
{
    /**
     * Set the new payload of the workflow instance.
     *
     * @param payload
     *            the payload (JSON) as stream
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    UpdatePayloadWorkflowInstanceCommandStep2 payload(InputStream payload);

    /**
     * Set the new payload of the workflow instance.
     *
     * @param payload
     *            the payload (JSON) as String
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    UpdatePayloadWorkflowInstanceCommandStep2 payload(String payload);

    interface UpdatePayloadWorkflowInstanceCommandStep2 extends FinalCommandStep<WorkflowInstanceEvent>
    {
        // the place for new optional parameters
    }
}
