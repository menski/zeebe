/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.system.deployment;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.Rule;
import org.junit.Test;

import io.zeebe.broker.clustering.orchestration.topic.TopicRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.WorkflowVersions;
import io.zeebe.broker.system.deployment.handler.RemoteWorkflowsManager;
import io.zeebe.broker.system.deployment.service.DeploymentManager;
import io.zeebe.broker.topic.StreamProcessorControl;
import io.zeebe.broker.util.StreamProcessorRule;
import io.zeebe.broker.workflow.data.DeploymentRecord;
import io.zeebe.broker.workflow.data.ResourceType;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TopicIntent;
import io.zeebe.util.buffer.BufferUtil;

public class CreateDeploymentStreamProcessorTest
{

    private static final String STREAM_NAME = "stream";
    protected static final WorkflowDefinition ONE_TASK_PROCESS =
        Bpmn.createExecutableWorkflow("foo")
            .startEvent()
            .serviceTask()
            .taskType("bar")
            .done()
            .endEvent()
            .done();
    private static final Duration DEPLOYMENT_TIMEOUT = Duration.ofSeconds(50);

    @Rule
    public StreamProcessorRule rule = new StreamProcessorRule();

    protected TypedStreamProcessor buildStreamProcessor(TypedStreamEnvironment env)
    {
        final WorkflowVersions versions = new WorkflowVersions();
        final PendingWorkflows pendingWorkflows = new PendingWorkflows();
        final PendingDeployments pendingDeployments = new PendingDeployments();

        final RemoteWorkflowsManager remoteManager = mock(RemoteWorkflowsManager.class);
        when(remoteManager.distributeWorkflow(any(), anyLong(), any())).thenReturn(true);
        when(remoteManager.deleteWorkflow(any(), anyLong(), any())).thenReturn(true);

        return DeploymentManager.createDeploymentStreamProcessor(
                versions,
                pendingDeployments,
                pendingWorkflows,
                DEPLOYMENT_TIMEOUT,
                env,
                remoteManager
            );
    }

    @Test
    public void shouldTimeOutDeploymentAfterStreamProcessorRestart()
    {
        // given
        rule.getClock().pinCurrentTime();

        final StreamProcessorControl control = rule.runStreamProcessor(this::buildStreamProcessor);

        control.blockAfterDeploymentEvent(e -> e.getMetadata().getIntent() == DeploymentIntent.VALIDATED);

        rule.writeEvent(TopicIntent.CREATED, topic(STREAM_NAME, 1));
        rule.writeCommand(DeploymentIntent.CREATE, deployment(ONE_TASK_PROCESS));

        waitUntil(() -> control.isBlocked());

        control.restart();

        // when
        rule.getClock().addTime(DEPLOYMENT_TIMEOUT.plus(Duration.ofSeconds(1)));

        // then
        waitUntil(() ->
            rule.events()
                .onlyDeploymentRecords()
                .withIntent(DeploymentIntent.TIMED_OUT)
                .count()
            > 0);
    }

    protected DeploymentRecord deployment(WorkflowDefinition workflow)
    {
        final DeploymentRecord event = new DeploymentRecord();

        event.setTopicName(STREAM_NAME);
        event.resources().add()
            .setResourceName(BufferUtil.wrapString("foo.bpmn"))
            .setResourceType(ResourceType.BPMN_XML)
            .setResource(BufferUtil.wrapString(Bpmn.convertToString(workflow)));

        return event;
    }

    protected TopicRecord topic(String name, int partitions)
    {
        final TopicRecord event = new TopicRecord();
        event.setName(BufferUtil.wrapString(name));
        event.setPartitions(partitions);
        for (int i = 0; i < partitions; i++)
        {
            event.getPartitionIds().add().setValue(i + 1);
        }

        return event;
    }
}
