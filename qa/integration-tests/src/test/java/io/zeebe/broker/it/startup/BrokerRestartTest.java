/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.startup;

import static io.zeebe.broker.it.util.TopicEventRecorder.incidentEvent;
import static io.zeebe.broker.it.util.TopicEventRecorder.jobEvent;
import static io.zeebe.broker.it.util.TopicEventRecorder.wfInstanceEvent;
import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.RecordingJobHandler;
import io.zeebe.broker.it.util.TopicEventRecorder;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.cmd.ClientCommandRejectedException;
import io.zeebe.client.event.DeploymentEvent;
import io.zeebe.client.event.TaskEvent;
import io.zeebe.client.event.WorkflowInstanceEvent;
import io.zeebe.client.impl.clustering.*;
import io.zeebe.client.impl.job.TaskSubscription;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftServiceNames;
import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.test.util.TestUtil;
import io.zeebe.transport.SocketAddress;

public class BrokerRestartTest
{

    private static final WorkflowDefinition WORKFLOW = Bpmn.createExecutableWorkflow("process")
            .startEvent("start")
            .serviceTask("task", t -> t.taskType("foo"))
            .endEvent("end")
            .done();

    private static final WorkflowDefinition WORKFLOW_TWO_TASKS = Bpmn.createExecutableWorkflow("process")
            .startEvent("start")
            .serviceTask("task1", t -> t.taskType("foo"))
            .serviceTask("task2", t -> t.taskType("bar"))
            .endEvent("end")
            .done();

    private static final WorkflowDefinition WORKFLOW_INCIDENT = Bpmn.createExecutableWorkflow("process")
            .startEvent("start")
            .serviceTask("task", t -> t.taskType("test")
                         .input("$.foo", "$.foo"))
            .endEvent("end")
            .done();

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule();

    public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule);

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule)
        .around(eventRecorder);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCreateWorkflowInstanceAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW, "workflow.bpmn")
            .execute();

        // when
        restartBroker();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        // then
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(wfInstanceEvent("WORKFLOW_INSTANCE_CREATED")));
    }

    @Test
    public void shouldContinueWorkflowInstanceAtTaskAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW, "workflow.bpmn")
            .execute();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("CREATED")));

        // when
        restartBroker();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler((c, t) -> c.complete(t).withoutPayload().execute())
            .open();

        // then
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("COMPLETED")));
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(wfInstanceEvent("WORKFLOW_INSTANCE_COMPLETED")));
    }

    @Test
    public void shouldContinueWorkflowInstanceWithLockedTaskAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW, "workflow.bpmn")
            .execute();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        final RecordingJobHandler recordingTaskHandler = new RecordingJobHandler();
        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler(recordingTaskHandler)
            .open();

        waitUntil(() -> !recordingTaskHandler.getHandledJobs().isEmpty());

        // when
        restartBroker();

        final TaskEvent task = recordingTaskHandler.getHandledJobs().get(0);
        clientRule.tasks().complete(task).execute();

        // then
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("COMPLETED")));
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(wfInstanceEvent("WORKFLOW_INSTANCE_COMPLETED")));
    }

    @Test
    public void shouldContinueWorkflowInstanceAtSecondTaskAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW_TWO_TASKS, "two-tasks.bpmn")
            .execute();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler((c, t) -> c.complete(t).withoutPayload().execute())
            .open();

        waitUntil(() -> eventRecorder.getJobEvents(jobEvent("CREATED")).size() > 1);

        // when
        restartBroker();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("bar")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler((c, t) -> c.complete(t).withoutPayload().execute())
            .open();

        // then
        waitUntil(() -> eventRecorder.getJobEvents(jobEvent("COMPLETED")).size() > 1);
        waitUntil(() -> eventRecorder.hasWorkflowInstanceEvent(wfInstanceEvent("WORKFLOW_INSTANCE_COMPLETED")));
    }

    @Test
    // FIXME: https://github.com/zeebe-io/zeebe/issues/567
    @Category(io.zeebe.UnstableTest.class)
    public void shouldDeployNewWorkflowVersionAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW, "workflow.bpmn")
            .execute();

        // when
        restartBroker();

        final DeploymentEvent deploymentResult = clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW, "workflow.bpmn")
            .execute();

        // then
        assertThat(deploymentResult.getDeployedWorkflows().get(0).getVersion()).isEqualTo(2);

        final WorkflowInstanceEvent workflowInstanceV1 = clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .version(1)
            .execute();

        final WorkflowInstanceEvent workflowInstanceV2 = clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .latestVersionForceRefresh()
            .execute();

        // then
        assertThat(workflowInstanceV1.getVersion()).isEqualTo(1);
        assertThat(workflowInstanceV2.getVersion()).isEqualTo(2);
    }

    @Test
    public void shouldLockAndCompleteStandaloneTaskAfterRestart()
    {
        // given
        clientRule.tasks().create(clientRule.getDefaultTopic(), "foo").execute();

        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("CREATED")));

        // when
        restartBroker();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler((c, t) -> c.complete(t).withoutPayload().execute())
            .open();

        // then
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("COMPLETED")));
    }

    @Test
    public void shouldCompleteStandaloneTaskAfterRestart()
    {
        // given
        clientRule.tasks().create(clientRule.getDefaultTopic(), "foo").execute();

        final RecordingJobHandler recordingTaskHandler = new RecordingJobHandler();
        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockOwner("test")
            .lockTime(Duration.ofSeconds(5))
            .handler(recordingTaskHandler)
            .open();

        waitUntil(() -> !recordingTaskHandler.getHandledJobs().isEmpty());

        // when
        restartBroker();

        final TaskEvent task = recordingTaskHandler.getHandledJobs().get(0);
        clientRule.tasks().complete(task).execute();

        // then
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("COMPLETED")));
    }

    @Test
    public void shouldNotReceiveLockedTasksAfterRestart()
    {
        // given
        clientRule.tasks().create(clientRule.getDefaultTopic(), "foo").execute();

        final RecordingJobHandler taskHandler = new RecordingJobHandler();
        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockTime(Duration.ofSeconds(5))
            .lockOwner("test")
            .handler(taskHandler)
            .open();

        waitUntil(() -> !taskHandler.getHandledJobs().isEmpty());

        // when
        restartBroker();

        taskHandler.clear();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockTime(Duration.ofMinutes(10L))
            .lockOwner("test")
            .handler(taskHandler)
            .open();

        // then
        TestUtil.doRepeatedly(() -> null)
            .whileConditionHolds((o) -> taskHandler.getHandledJobs().isEmpty());

        assertThat(taskHandler.getHandledJobs()).isEmpty();
    }

    @Test
    public void shouldReceiveLockExpiredTasksAfterRestart()
    {
        // given
        clientRule.tasks().create(clientRule.getDefaultTopic(), "foo").execute();

        final RecordingJobHandler recordingTaskHandler = new RecordingJobHandler();
        final TaskSubscription subscription = clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockTime(Duration.ofSeconds(5))
            .lockOwner("test")
            .handler(recordingTaskHandler)
            .open();

        waitUntil(() -> !recordingTaskHandler.getHandledJobs().isEmpty());
        subscription.close();

        // when
        restartBroker();

        doRepeatedly(() ->
        {
            brokerRule.getClock().addTime(Duration.ofSeconds(60)); // retriggers lock expiration check in broker
            return null;
        }).until(t -> eventRecorder.hasJobEvent(jobEvent("LOCK_EXPIRED")));
        recordingTaskHandler.clear();

        clientRule.tasks().newTaskSubscription(clientRule.getDefaultTopic())
            .jobType("foo")
            .lockTime(Duration.ofMinutes(10L))
            .lockOwner("test")
            .handler(recordingTaskHandler)
            .open();

        // then
        waitUntil(() -> !recordingTaskHandler.getHandledJobs().isEmpty());

        final TaskEvent task = recordingTaskHandler.getHandledJobs().get(0);
        clientRule.tasks().complete(task).execute();

        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("COMPLETED")));
    }

    @Test
    public void shouldResolveIncidentAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW_INCIDENT, "incident.bpmn")
            .execute();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        waitUntil(() -> eventRecorder.hasIncidentEvent(incidentEvent("CREATED")));

        final WorkflowInstanceEvent activityInstance =
                eventRecorder.getSingleWorkflowInstanceEvent(TopicEventRecorder.wfInstanceEvent("ACTIVITY_READY"));

        // when
        restartBroker();

        clientRule.workflows().updatePayload(activityInstance)
            .payload("{\"foo\":\"bar\"}")
            .execute();

        // then
        waitUntil(() -> eventRecorder.hasIncidentEvent(incidentEvent("RESOLVED")));
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("CREATED")));
    }

    @Test
    public void shouldResolveFailedIncidentAfterRestart()
    {
        // given
        clientRule.workflows().deploy(clientRule.getDefaultTopic())
            .addWorkflowModel(WORKFLOW_INCIDENT, "incident.bpmn")
            .execute();

        clientRule.workflows().create(clientRule.getDefaultTopic())
            .bpmnProcessId("process")
            .execute();

        waitUntil(() -> eventRecorder.hasIncidentEvent(incidentEvent("CREATED")));

        final WorkflowInstanceEvent activityInstance =
                eventRecorder.getSingleWorkflowInstanceEvent(TopicEventRecorder.wfInstanceEvent("ACTIVITY_READY"));

        clientRule.workflows().updatePayload(activityInstance)
            .payload("{\"x\":\"y\"}")
            .execute();

        waitUntil(() -> eventRecorder.hasIncidentEvent(incidentEvent("RESOLVE_FAILED")));

        // when
        restartBroker();

        clientRule.workflows().updatePayload(activityInstance)
            .payload("{\"foo\":\"bar\"}")
            .execute();

        // then
        waitUntil(() -> eventRecorder.hasIncidentEvent(incidentEvent("RESOLVED")));
        waitUntil(() -> eventRecorder.hasJobEvent(jobEvent("CREATED")));
    }

    @Test
    public void shouldLoadRaftConfiguration()
    {
        // given
        final int testTerm = 8;

        final ServiceName<Raft> serviceName = RaftServiceNames.raftServiceName(clientRule.getDefaultTopic() + "-" + clientRule.getDefaultPartition());

        final Raft raft = brokerRule.getService(serviceName);
        waitUntil(() -> raft.getState() == RaftState.LEADER);

        raft.setTerm(testTerm);

        // when
        restartBroker();

        final Raft raftAfterRestart = brokerRule.getService(serviceName);
        waitUntil(() -> raftAfterRestart.getState() == RaftState.LEADER);

        // then
        assertThat(raftAfterRestart.getState()).isEqualTo(RaftState.LEADER);
        assertThat(raftAfterRestart.getTerm()).isGreaterThanOrEqualTo(9);
        assertThat(raftAfterRestart.getMemberSize()).isEqualTo(0);
        assertThat(raftAfterRestart.getVotedFor()).isEqualTo(new SocketAddress("0.0.0.0", 51017));
    }

    @Test
    public void shouldCreateTopicAfterRestart()
    {
        // given
        final ZeebeClient client = clientRule.getClient();
        restartBroker();

        // when
        client.topics().create("foo", 2).execute();

        // then
        final TaskEvent taskEvent = client.tasks().create("foo", "bar").execute();
        assertThat(taskEvent.getState()).isEqualTo("CREATED");
    }


    @Test
    public void shouldNotCreatePreviouslyCreatedTopicAfterRestart()
    {
        // given
        final ZeebeClient client = clientRule.getClient();

        final String topicName = "foo";
        client.topics().create(topicName, 2).execute();

        restartBroker();

        // then
        exception.expect(ClientCommandRejectedException.class);

        // when
        client.topics().create(topicName, 2).execute();
    }

    @Test
    public void shouldCreateUniquePartitionIdsAfterRestart()
    {
        // given
        final ZeebeClient client = clientRule.getClient();

        client.topics().create("foo", 2).execute();
        clientRule.waitUntilTopicsExists("foo");

        restartBroker();

        // when
        client.topics().create("bar", 2).execute();
        clientRule.waitUntilTopicsExists("bar");

        // then
        final TopologyImpl topology = client.requestTopology().execute();
        final List<BrokerInfoImpl> brokers = topology.getBrokers();
        assertThat(brokers).hasSize(1);

        final BrokerInfoImpl topologyBroker = brokers.get(0);
        final List<PartitionInfoImpl> partitions = topologyBroker.getPartitions();

        assertThat(partitions).hasSize(6); // default partition + system partition + 4 partitions we create here
        assertThat(partitions).extracting("partitionId").doesNotHaveDuplicates();
    }

    protected void restartBroker()
    {
        restartBroker(() ->
        { });
    }

    protected void restartBroker(Runnable onStop)
    {
        eventRecorder.stopRecordingEvents();
        brokerRule.stopBroker();

        onStop.run();

        brokerRule.startBroker();
        eventRecorder.startRecordingEvents();
    }

}
