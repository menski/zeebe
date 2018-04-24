package io.zeebe.client.api.commands;

public interface Workflow
{
    /**
     * @return the BPMN process id of the workflow
     */
    String getBpmnProcessId();

    /**
     * @return the version of the deployed workflow
     */
    int getVersion();

}
