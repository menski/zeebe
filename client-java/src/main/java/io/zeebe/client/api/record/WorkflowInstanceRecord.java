package io.zeebe.client.api.record;

public interface WorkflowInstanceRecord extends Record
{
    /**
     * @return the BPMN process id this workflow instance belongs to.
     */
    String getBpmnProcessId();

    /**
     * @return the version of the deployed workflow this instance belongs to.
     */
    int getVersion();

    /**
     * @return the key of the deployed workflow this instance belongs to.
     */
    long getWorkflowKey();

    /**
     * @return the key of the workflow instance
     */
    long getWorkflowInstanceKey();

    /**
     * @return the id of the current activity, or empty if the event does not
     *         belong to an activity.
     */
    String getActivityId();

    /**
     * @return the payload of the workflow instance as JSON-formatted string.
     */
    String getPayload();
}
