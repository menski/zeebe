package io.zeebe.client.api.record;

public interface IncidentRecord extends Record
{
    /**
     * @return the type of error this incident is caused by.
     */
    String getErrorType();

    /**
     * @return the description of the error this incident is caused by.
     */
    String getErrorMessage();

    /**
     * @return the BPMN process id this incident belongs to. Can be <code>null</code> if the
     *         incident belongs to no workflow instance.
     */
    String getBpmnProcessId();

    /**
     * @return the key of the workflow instance this incident belongs to. Can be
     *         <code>null</code> if the incident belongs to no workflow instance.
     */
    Long getWorkflowInstanceKey();

    /**
     * @return the id of the activity this incident belongs to. Can be <code>null</code> if
     *         the incident belongs to no activity or workflow instance.
     */
    String getActivityId();

    /**
     * @return the key of the activity instance this incident belongs to. Can be
     *         <code>null</code> if the incident belongs to no activity or workflow
     *         instance.
     */
    Long getActivityInstanceKey();

    /**
     * @return the key of the job this incident belongs to. Can be <code>null</code> if the
     *         incident belongs to no task.
     */
    Long getJobKey();
}
