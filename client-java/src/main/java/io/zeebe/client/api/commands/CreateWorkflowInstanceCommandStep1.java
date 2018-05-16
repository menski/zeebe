package io.zeebe.client.api.commands;

import java.io.InputStream;

import io.zeebe.client.api.events.WorkflowInstanceEvent;

public interface CreateWorkflowInstanceCommandStep1
{
    /**
     * Use the latest version of the workflow (without guarantee).
     */
    int LATEST_VERSION = -1;

    /**
     * Use the latest version of the workflow (with guarantee).
     */
    int FORCE_LATEST_VERSION = -2;

    /**
     * Set the BPMN process id of the workflow to create an instance of. This is
     * the static id of the process in the BPMN XML (i.e. "&#60;bpmn:process
     * id='my-workflow'&#62;").
     *
     * @param bpmnProcessId
     *            the BPMN process id of the workflow
     * @return the builder for this command
     */
    CreateWorkflowInstanceCommandStep2 bpmnProcessId(String bpmnProcessId);

    /**
     * Set the key of the workflow to create an instance of. The key is assigned
     * by the broker while deploying the workflow. It can be picked from the
     * deployment or workflow event.
     *
     * @param workflowKey
     *            the key of the workflow
     * @return the builder for this command
     */
    CreateWorkflowInstanceCommandStep3 workflowKey(long workflowKey);

    interface CreateWorkflowInstanceCommandStep2
    {
        /**
         * Set the version of the workflow to create an instance of. The version
         * is assigned by the broker while deploying the workflow. It can be
         * picked from the deployment or workflow event.
         *
         * @param version
         *            the version of the workflow
         * @return the builder for this command
         */
        CreateWorkflowInstanceCommandStep3 version(int version);

        /**
         * Use the latest version of the workflow to create an instance of.
         * <p>
         * If the latest version was deployed few moments before then it can
         * happen that the new instance is created of the previous version. Use
         * {@link #latestVersionForce()} to force the latest version in any
         * case.
         *
         * @return the builder for this command
         */
        CreateWorkflowInstanceCommandStep3 latestVersion();

        /**
         * Use the latest version of the workflow to create an instance of.
         * <p>
         * In contrast to {@link #latestVersion()}, it's guaranteed that the new
         * instance is always created of the latest version.
         *
         * @return the builder for this command
         */
        CreateWorkflowInstanceCommandStep3 latestVersionForce();
    }

    interface CreateWorkflowInstanceCommandStep3 extends FinalCommandStep<WorkflowInstanceEvent>
    {
        /**
         * Set the initial payload of the workflow instance.
         *
         * @param payload
         *            the payload (JSON) as stream
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateWorkflowInstanceCommandStep3 payload(InputStream payload);

        /**
         * Set the initial payload of the workflow instance.
         *
         * @param payload
         *            the payload (JSON) as String
         *
         * @return the builder for this command. Call {@link #send()} to
         *         complete the command and send it to the broker.
         */
        CreateWorkflowInstanceCommandStep3 payload(String payload);
    }

}
