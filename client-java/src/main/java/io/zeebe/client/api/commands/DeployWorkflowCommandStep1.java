package io.zeebe.client.api.commands;

import java.io.InputStream;
import java.nio.charset.Charset;

import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;

public interface DeployWorkflowCommandStep1
{

    /**
     * Add the given resource to the deployment.
     *
     * @param resourceBytes
     *            the workflow resource as byte array
     * @param resourceName
     *            the name of the resource (e.g. "workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceBytes(byte[] resourceBytes, String resourceName);

    /**
     * Add the given resource to the deployment.
     *
     * @param resourceString
     *            the workflow resource as String
     * @param charset
     *            the charset of the String
     * @param resourceName
     *            the name of the resource (e.g. "workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceString(String resourceString, Charset charset, String resourceName);

    /**
     * Add the given resource to the deployment.
     *
     * @param resourceString
     *            the workflow resource as UTF-8-encoded String
     * @param resourceName
     *            the name of the resource (e.g. "workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceStringUtf8(String resourceString, String resourceName);

    /**
     * Add the given resource to the deployment.
     *
     * @param resourceStream
     *            the workflow resource as stream
     * @param resourceName
     *            the name of the resource (e.g. "workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceStream(InputStream resourceStream, String resourceName);

    /**
     * Add the given resource to the deployment.
     *
     * @param classpathResource
     *            the path of the workflow resource in the classpath (e.g.
     *            "wf/workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceFromClasspath(String classpathResource);

    /**
     * Add the given resource to the deployment.
     *
     * @param filename
     *            the absolute path of the workflow resource (e.g.
     *            "~/wf/workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addResourceFile(String filename);

    /**
     * Add the given workflow to the deployment.
     *
     * @param workflowDefinition
     *            the workflow as model
     * @param resourceName
     *            the name of the resource (e.g. "workflow.bpmn")
     *
     * @return the builder for this command. Call {@link #send()} to
     *         complete the command and send it to the broker.
     */
    DeployWorkflowCommandBuilderStep2 addWorkflowModel(WorkflowDefinition workflowDefinition, String resourceName);

    interface DeployWorkflowCommandBuilderStep2 extends DeployWorkflowCommandStep1, FinalCommandStep<DeploymentEvent>
    {
        // the place for new optional parameters
    }
}
