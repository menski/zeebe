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
package io.zeebe.broker.workflow.processor;

import static io.zeebe.broker.util.PayloadUtil.isNilPayload;
import static io.zeebe.broker.util.PayloadUtil.isValidPayload;

import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;

import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.deployment.handler.CreateWorkflowResponseSender;
import io.zeebe.broker.task.data.TaskRecord;
import io.zeebe.broker.task.data.TaskHeaders;
import io.zeebe.broker.workflow.data.WorkflowRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.ActivityInstanceMap;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.PayloadCache;
import io.zeebe.broker.workflow.map.WorkflowDeploymentCache;
import io.zeebe.broker.workflow.map.WorkflowInstanceIndex;
import io.zeebe.broker.workflow.map.WorkflowInstanceIndex.WorkflowInstance;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.model.bpmn.BpmnAspect;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ExclusiveGateway;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.model.bpmn.instance.FlowNode;
import io.zeebe.model.bpmn.instance.SequenceFlow;
import io.zeebe.model.bpmn.instance.ServiceTask;
import io.zeebe.model.bpmn.instance.StartEvent;
import io.zeebe.model.bpmn.instance.TaskDefinition;
import io.zeebe.model.bpmn.instance.Workflow;
import io.zeebe.msgpack.el.CompiledJsonCondition;
import io.zeebe.msgpack.el.JsonConditionException;
import io.zeebe.msgpack.el.JsonConditionInterpreter;
import io.zeebe.msgpack.mapping.Mapping;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.msgpack.mapping.MappingProcessor;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.TaskIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.intent.WorkflowIntent;
import io.zeebe.util.metrics.Metric;
import io.zeebe.util.metrics.MetricsManager;

public class WorkflowInstanceStreamProcessor implements StreamProcessorLifecycleAware
{
    private static final UnsafeBuffer EMPTY_TASK_TYPE = new UnsafeBuffer("".getBytes());

    private final Map<BpmnAspect, TypedRecordProcessor<WorkflowInstanceRecord>> aspectHandlers;

    private Metric workflowInstanceEventCreate;
    private Metric workflowInstanceEventCanceled;
    private Metric workflowInstanceEventCompleted;

    {
        aspectHandlers = new EnumMap<>(BpmnAspect.class);

        aspectHandlers.put(BpmnAspect.TAKE_SEQUENCE_FLOW, new TakeSequenceFlowAspectHandler());
        aspectHandlers.put(BpmnAspect.CONSUME_TOKEN, new ConsumeTokenAspectHandler());
        aspectHandlers.put(BpmnAspect.EXCLUSIVE_SPLIT, new ExclusiveSplitAspectHandler());
    }

    private final WorkflowInstanceIndex workflowInstanceIndex = new WorkflowInstanceIndex();
    private final ActivityInstanceMap activityInstanceMap = new ActivityInstanceMap();
    private final WorkflowDeploymentCache workflowDeploymentCache;
    private final PayloadCache payloadCache;

    private final MappingProcessor payloadMappingProcessor = new MappingProcessor(4096);
    private final JsonConditionInterpreter conditionInterpreter = new JsonConditionInterpreter();

    private final CreateWorkflowResponseSender workflowResponseSender;

    public WorkflowInstanceStreamProcessor(
            CreateWorkflowResponseSender createWorkflowResponseSender,
            int deploymentCacheSize,
            int payloadCacheSize)
    {
        this.workflowDeploymentCache = new WorkflowDeploymentCache(deploymentCacheSize);
        this.payloadCache = new PayloadCache(payloadCacheSize);
        this.workflowResponseSender = createWorkflowResponseSender;
    }

    public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment)
    {
        return environment.newStreamProcessor()
            .onCommand(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CREATE, new CreateWorkflowInstanceEventProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CREATED, new WorkflowInstanceCreatedEventProcessor())
            .onCommand(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CANCEL, new CancelWorkflowInstanceProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, w -> isActive(w.getWorkflowInstanceKey()), new SequenceFlowTakenEventProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ACTIVITY_READY, w -> isActive(w.getWorkflowInstanceKey()), new ActivityReadyEventProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ACTIVITY_ACTIVATED, w -> isActive(w.getWorkflowInstanceKey()), new ActivityActivatedEventProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ACTIVITY_COMPLETING, w -> isActive(w.getWorkflowInstanceKey()), new ActivityCompletingEventProcessor())
            .onCommand(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.UPDATE_PAYLOAD, new UpdatePayloadProcessor())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.START_EVENT_OCCURRED, this::getBpmnAspectHandler)
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.END_EVENT_OCCURRED, this::getBpmnAspectHandler)
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.GATEWAY_ACTIVATED, this::getBpmnAspectHandler)
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.ACTIVITY_COMPLETED, this::getBpmnAspectHandler)
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.CANCELED, (Consumer<WorkflowInstanceEvent>) (e) -> workflowInstanceEventCanceled.incrementOrdered())
            .onEvent(ValueType.WORKFLOW_INSTANCE, WorkflowInstanceIntent.COMPLETED, (Consumer<WorkflowInstanceEvent>) (e) -> workflowInstanceEventCompleted.incrementOrdered())

            .onEvent(ValueType.TASK, TaskIntent.CREATED, new TaskCreatedProcessor())
            .onEvent(ValueType.TASK, TaskIntent.COMPLETED, new TaskCompletedEventProcessor())

            .onCommand(ValueType.WORKFLOW, WorkflowIntent.CREATE, new WorkflowCreateEventProcessor())
            .onCommand(ValueType.WORKFLOW, WorkflowIntent.DELETE, new WorkflowDeleteEventProcessor())

            .withStateResource(workflowInstanceIndex.getMap())
            .withStateResource(activityInstanceMap.getMap())
            .withStateResource(workflowDeploymentCache.getIdVersionToKeyMap())
            .withStateResource(workflowDeploymentCache.getKeyToPositionWorkflowMap())
            .withStateResource(payloadCache.getMap())

            .withListener(workflowDeploymentCache)
            .withListener(payloadCache)
            .withListener(this)
            .build();
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        final StreamProcessorContext context = streamProcessor.getStreamProcessorContext();
        final LogStream logStream = context.getLogStream();
        final MetricsManager metricsManager = context.getActorScheduler().getMetricsManager();
        final String topicName = logStream.getTopicName().getStringWithoutLengthUtf8(0, logStream.getTopicName().capacity());
        final String partitionId = Integer.toString(logStream.getPartitionId());

        workflowInstanceEventCreate = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "created")
            .create();

        workflowInstanceEventCanceled = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "canceled")
            .create();

        workflowInstanceEventCompleted = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "completed")
            .create();
    }

    @Override
    public void onClose()
    {
        workflowInstanceEventCreate.close();
        workflowInstanceEventCanceled.close();
        workflowInstanceEventCompleted.close();
    }

    private <T extends FlowElement> T getActivity(long workflowKey, DirectBuffer activityId)
    {
        final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);

        if (deployedWorkflow != null)
        {
            final Workflow workflow = deployedWorkflow.getWorkflow();
            return workflow.findFlowElementById(activityId);
        }
        else
        {
            throw new RuntimeException("No workflow found for key: " + workflowKey);
        }
    }

    private boolean isActive(long workflowInstanceKey)
    {
        final WorkflowInstance workflowInstance = workflowInstanceIndex.get(workflowInstanceKey);
        return workflowInstance != null && workflowInstance.getTokenCount() > 0;
    }

    private TypedRecordProcessor<WorkflowInstanceRecord> getBpmnAspectHandler(WorkflowInstanceRecord workflowInstanceEvent)
    {
        final boolean isActive = isActive(workflowInstanceEvent.getWorkflowInstanceKey());

        if (isActive)
        {
            final FlowNode currentActivity = getActivity(workflowInstanceEvent.getWorkflowKey(), workflowInstanceEvent.getActivityId());
            return aspectHandlers.get(currentActivity.getBpmnAspect());
        }
        else
        {
            return null;
        }
    }

    private final class WorkflowCreateEventProcessor implements TypedRecordProcessor<WorkflowRecord>
    {
        private boolean isNewWorkflow;
        private int partitionId;

        @Override
        public void onOpen(TypedStreamProcessor streamProcessor)
        {
            partitionId = streamProcessor.getEnvironment().getStream().getPartitionId();
        }

        @Override
        public void processRecord(TypedRecord<WorkflowRecord> command)
        {
            isNewWorkflow = !workflowDeploymentCache.hasWorkflow(command.getKey());
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowRecord> command, TypedResponseWriter responseWriter)
        {
            return workflowResponseSender.sendCreateWorkflowResponse(
                    partitionId,
                    command.getKey(),
                    command.getValue().getDeploymentKey(),
                    command.getMetadata().getRequestId(),
                    command.getMetadata().getRequestStreamId());
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowRecord> command, TypedStreamWriter writer)
        {
            if (isNewWorkflow)
            {
                return writer.writeFollowUpEvent(command.getKey(), WorkflowIntent.CREATED, command.getValue());
            }
            else
            {
                return writer.writeRejection(command);
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowRecord> command)
        {
            if (isNewWorkflow)
            {
                workflowDeploymentCache.addDeployedWorkflow(command.getPosition(), command.getKey(), command.getValue());
            }
        }
    }

    private final class WorkflowDeleteEventProcessor implements TypedRecordProcessor<WorkflowRecord>
    {

        private final WorkflowInstanceRecord workflowInstanceRecord = new WorkflowInstanceRecord();
        private boolean isDeleted;
        private final LongArrayList workflowInstanceKeys = new LongArrayList();

        @Override
        public void processRecord(TypedRecord<WorkflowRecord> command)
        {
            isDeleted = workflowDeploymentCache.getWorkflow(command.getKey()) != null;

            if (isDeleted)
            {
                collectInstancesOfWorkflow(command.getKey());
            }
        }

        private void collectInstancesOfWorkflow(long workflowKey)
        {
            workflowInstanceKeys.clear();

            final Iterator<WorkflowInstance> workflowInstances = workflowInstanceIndex.iterator();
            while (workflowInstances.hasNext())
            {
                final WorkflowInstance workflowInstance = workflowInstances.next();

                if (workflowKey == workflowInstance.getWorkflowKey())
                {
                    workflowInstanceKeys.addLong(workflowInstance.getKey());
                }
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowRecord> command, TypedStreamWriter writer)
        {
            if (isDeleted)
            {
                final TypedBatchWriter batchWriter = writer.newBatch();

                batchWriter.addFollowUpEvent(command.getKey(), WorkflowIntent.DELETED, command.getValue());

                if (!workflowInstanceKeys.isEmpty())
                {
                    final WorkflowRecord workflowEvent = command.getValue();

                    workflowInstanceRecord
                        .setWorkflowKey(command.getKey())
                        .setBpmnProcessId(workflowEvent.getBpmnProcessId())
                        .setVersion(workflowEvent.getVersion());

                    for (int i = 0; i < workflowInstanceKeys.size(); i++)
                    {
                        final long workflowInstanceKey = workflowInstanceKeys.getLong(i);
                        workflowInstanceRecord.setWorkflowInstanceKey(workflowInstanceKey);

                        batchWriter.addFollowUpEvent(workflowInstanceKey, WorkflowInstanceIntent.CANCEL, workflowInstanceRecord);
                    }
                }

                return batchWriter.write();
            }
            else
            {
                return 0L;
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowRecord> command)
        {
            if (isDeleted)
            {
                final WorkflowRecord value = command.getValue();
                final DirectBuffer bpmnProcessId = value.getBpmnProcessId();
                final int version = value.getVersion();

                workflowDeploymentCache.removeDeployedWorkflow(command.getKey(), bpmnProcessId, version);
            }
        }
    }

    private final class CreateWorkflowInstanceEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private boolean success;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> command)
        {
            success = false;

            final WorkflowInstanceRecord workflowInstanceEvent = command.getValue();

            long workflowKey = workflowInstanceEvent.getWorkflowKey();
            final DirectBuffer bpmnProcessId = workflowInstanceEvent.getBpmnProcessId();
            final int version = workflowInstanceEvent.getVersion();

            if (workflowKey <= 0)
            {
                if (version > 0)
                {
                    workflowKey = workflowDeploymentCache.getWorkflowKeyByIdAndVersion(bpmnProcessId, version);
                }
                else
                {
                    workflowKey = workflowDeploymentCache.getWorkflowKeyByIdAndLatestVersion(bpmnProcessId);
                }
            }

            if (workflowKey > 0)
            {
                final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);
                final DirectBuffer payload = workflowInstanceEvent.getPayload();

                if (deployedWorkflow != null && (isNilPayload(payload) || isValidPayload(payload)))
                {
                    workflowInstanceEvent
                        .setWorkflowKey(workflowKey)
                        .setBpmnProcessId(deployedWorkflow.getWorkflow().getBpmnProcessId())
                        .setVersion(deployedWorkflow.getVersion())
                        .setWorkflowInstanceKey(command.getKey());

                    success = true;
                }
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowInstanceRecord> command, TypedResponseWriter responseWriter)
        {
            if (success)
            {
                return responseWriter.writeEvent(WorkflowInstanceIntent.CREATED, command);
            }
            else
            {
                return responseWriter.writeRejection(command);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> command, TypedStreamWriter writer)
        {
            if (success)
            {
                return writer.writeFollowUpEvent(command.getKey(), WorkflowInstanceIntent.CREATED, command.getValue());
            }
            else
            {
                return writer.writeRejection(command);
            }
        }
    }

    private final class WorkflowInstanceCreatedEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            workflowInstanceEventCreate.incrementOrdered();

            final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

            final long workflowKey = workflowInstanceEvent.getWorkflowKey();
            final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);

            if (deployedWorkflow != null)
            {
                final Workflow workflow = deployedWorkflow.getWorkflow();
                final StartEvent startEvent = workflow.getInitialStartEvent();
                final DirectBuffer activityId = startEvent.getIdAsBuffer();

                workflowInstanceEvent
                    .setWorkflowInstanceKey(record.getKey())
                    .setActivityId(activityId);
            }
            else
            {
                throw new RuntimeException("No workflow found for key: " + workflowKey);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            return writer.writeNewEvent(WorkflowInstanceIntent.START_EVENT_OCCURRED, record.getValue());
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> record)
        {
            workflowInstanceIndex
                .newWorkflowInstance(record.getKey())
                .setPosition(record.getPosition())
                .setActiveTokenCount(1)
                .setActivityInstanceKey(-1L)
                .setWorkflowKey(record.getValue().getWorkflowKey())
                .write();
        }
    }

    private final class TakeSequenceFlowAspectHandler implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

            final FlowNode flowNode = getActivity(
                    workflowInstanceEvent.getWorkflowKey(), workflowInstanceEvent.getActivityId());

            // the activity has exactly one outgoing sequence flow
            final SequenceFlow sequenceFlow = flowNode.getOutgoingSequenceFlows().get(0);

            workflowInstanceEvent.setActivityId(sequenceFlow.getIdAsBuffer());
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            return writer.writeNewEvent(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, record.getValue());
        }
    }

    private final class ExclusiveSplitAspectHandler implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private boolean createsIncident;
        private boolean isResolvingIncident;
        private final IncidentRecord incidentCommand = new IncidentRecord();

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

            createsIncident = false;
            isResolvingIncident = record.getMetadata().hasIncidentKey();

            final ExclusiveGateway exclusiveGateway = getActivity(workflowInstanceEvent.getWorkflowKey(), workflowInstanceEvent.getActivityId());

            try
            {
                final SequenceFlow sequenceFlow = getSequenceFlowWithFulfilledCondition(exclusiveGateway, workflowInstanceEvent.getPayload());

                if (sequenceFlow != null)
                {
                    workflowInstanceEvent.setActivityId(sequenceFlow.getIdAsBuffer());
                }
                else
                {
                    incidentCommand.reset();
                    incidentCommand
                        .initFromWorkflowInstanceFailure(record)
                        .setErrorType(ErrorType.CONDITION_ERROR)
                        .setErrorMessage("All conditions evaluated to false and no default flow is set.");

                    createsIncident = true;
                }
            }
            catch (JsonConditionException e)
            {
                incidentCommand.reset();

                incidentCommand
                    .initFromWorkflowInstanceFailure(record)
                    .setErrorType(ErrorType.CONDITION_ERROR)
                    .setErrorMessage(e.getMessage());

                createsIncident = true;
            }
        }

        private SequenceFlow getSequenceFlowWithFulfilledCondition(ExclusiveGateway exclusiveGateway, DirectBuffer payload)
        {
            final List<SequenceFlow> sequenceFlows = exclusiveGateway.getOutgoingSequenceFlowsWithConditions();
            for (int s = 0; s < sequenceFlows.size(); s++)
            {
                final SequenceFlow sequenceFlow = sequenceFlows.get(s);

                final CompiledJsonCondition compiledCondition = sequenceFlow.getCondition();
                final boolean isFulFilled = conditionInterpreter.eval(compiledCondition.getCondition(), payload);

                if (isFulFilled)
                {
                    return sequenceFlow;
                }
            }
            return exclusiveGateway.getDefaultFlow();
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            if (!createsIncident)
            {
                return writer.writeNewEvent(WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, record.getValue());
            }
            else
            {
                if (!isResolvingIncident)
                {
                    return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
                }
                else
                {
                    return writer.writeFollowUpEvent(record.getMetadata().getIncidentKey(), IncidentIntent.RESOLVE_FAILED, incidentCommand);
                }
            }
        }
    }

    private final class ConsumeTokenAspectHandler implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private boolean isCompleted;
        private int activeTokenCount;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

            final WorkflowInstance workflowInstance = workflowInstanceIndex
                    .get(workflowInstanceEvent.getWorkflowInstanceKey());

            activeTokenCount = workflowInstance != null ? workflowInstance.getTokenCount() : 0;
            isCompleted = activeTokenCount == 1;
            if (isCompleted)
            {
                workflowInstanceEvent.setActivityId("");
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            if (isCompleted)
            {
                return writer.writeFollowUpEvent(record.getValue().getWorkflowInstanceKey(), WorkflowInstanceIntent.COMPLETED, record.getValue());
            }
            else
            {
                return 0L;
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> record)
        {
            if (isCompleted)
            {
                final long workflowInstanceKey = record.getValue().getWorkflowInstanceKey();
                workflowInstanceIndex.remove(workflowInstanceKey);
                payloadCache.remove(workflowInstanceKey);
            }
        }
    }

    private final class SequenceFlowTakenEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private Intent nextState;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord sequenceFlowEvent = record.getValue();

            final SequenceFlow sequenceFlow = getActivity(sequenceFlowEvent.getWorkflowKey(), sequenceFlowEvent.getActivityId());
            final FlowNode targetNode = sequenceFlow.getTargetNode();

            sequenceFlowEvent.setActivityId(targetNode.getIdAsBuffer());

            if (targetNode instanceof EndEvent)
            {
                nextState = WorkflowInstanceIntent.END_EVENT_OCCURRED;
            }
            else if (targetNode instanceof ServiceTask)
            {
                nextState = WorkflowInstanceIntent.ACTIVITY_READY;
            }
            else if (targetNode instanceof ExclusiveGateway)
            {
                nextState = WorkflowInstanceIntent.GATEWAY_ACTIVATED;
            }
            else
            {
                throw new RuntimeException(String.format("Flow node of type '%s' is not supported.", targetNode));
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            return writer.writeNewEvent(nextState, record.getValue());
        }
    }

    private final class ActivityReadyEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private final IncidentRecord incidentCommand = new IncidentRecord();

        private boolean createsIncident;
        private boolean isResolvingIncident;
        private UnsafeBuffer wfInstancePayload = new UnsafeBuffer(0, 0);

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            createsIncident = false;
            isResolvingIncident = record.getMetadata().hasIncidentKey();

            final WorkflowInstanceRecord activityEvent = record.getValue();
            wfInstancePayload.wrap(activityEvent.getPayload());

            final ServiceTask serviceTask = getActivity(activityEvent.getWorkflowKey(), activityEvent.getActivityId());
            final Mapping[] inputMappings = serviceTask.getInputOutputMapping().getInputMappings();

            // only if we have no default mapping we have to use the mapping processor
            if (inputMappings.length > 0)
            {
                try
                {
                    final int resultLen = payloadMappingProcessor.extract(activityEvent.getPayload(), inputMappings);
                    final MutableDirectBuffer mappedPayload = payloadMappingProcessor.getResultBuffer();
                    activityEvent.setPayload(mappedPayload, 0, resultLen);
                }
                catch (MappingException e)
                {
                    incidentCommand.reset();

                    incidentCommand
                        .initFromWorkflowInstanceFailure(record)
                        .setErrorType(ErrorType.IO_MAPPING_ERROR)
                        .setErrorMessage(e.getMessage());

                    createsIncident = true;
                }
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            if (!createsIncident)
            {
                return writer.writeFollowUpEvent(record.getKey(), WorkflowInstanceIntent.ACTIVITY_ACTIVATED, record.getValue());
            }
            else
            {
                if (!isResolvingIncident)
                {
                    return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
                }
                else
                {
                    return writer.writeFollowUpEvent(record.getMetadata().getIncidentKey(), IncidentIntent.RESOLVE_FAILED, incidentCommand);
                }
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord workflowInstanceEvent = record.getValue();

            workflowInstanceIndex
                .get(workflowInstanceEvent.getWorkflowInstanceKey())
                .setActivityInstanceKey(record.getKey())
                .write();

            activityInstanceMap
                .newActivityInstance(record.getKey())
                .setActivityId(workflowInstanceEvent.getActivityId())
                .setTaskKey(-1L)
                .write();

            if (!createsIncident && !isNilPayload(workflowInstanceEvent.getPayload()))
            {
                payloadCache.addPayload(
                        workflowInstanceEvent.getWorkflowInstanceKey(),
                        record.getPosition(),
                        wfInstancePayload);
            }
        }
    }

    private final class ActivityActivatedEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private final TaskRecord taskCommand = new TaskRecord();

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            final WorkflowInstanceRecord activityEvent = record.getValue();

            final ServiceTask serviceTask = getActivity(activityEvent.getWorkflowKey(), activityEvent.getActivityId());
            final TaskDefinition taskDefinition = serviceTask.getTaskDefinition();

            taskCommand.reset();

            taskCommand
                .setType(taskDefinition.getTypeAsBuffer())
                .setRetries(taskDefinition.getRetries())
                .setPayload(activityEvent.getPayload())
                .headers()
                    .setBpmnProcessId(activityEvent.getBpmnProcessId())
                    .setWorkflowDefinitionVersion(activityEvent.getVersion())
                    .setWorkflowKey(activityEvent.getWorkflowKey())
                    .setWorkflowInstanceKey(activityEvent.getWorkflowInstanceKey())
                    .setActivityId(serviceTask.getIdAsBuffer())
                    .setActivityInstanceKey(record.getKey());

            final io.zeebe.model.bpmn.instance.TaskHeaders customHeaders = serviceTask.getTaskHeaders();

            if (!customHeaders.isEmpty())
            {
                taskCommand.setCustomHeaders(customHeaders.asMsgpackEncoded());
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            return writer.writeNewCommand(TaskIntent.CREATE, taskCommand);
        }
    }

    private final class TaskCreatedProcessor implements TypedRecordProcessor<TaskRecord>
    {
        private boolean isActive;

        @Override
        public void processRecord(TypedRecord<TaskRecord> record)
        {
            isActive = false;

            final TaskHeaders taskHeaders = record.getValue().headers();
            final long activityInstanceKey = taskHeaders.getActivityInstanceKey();
            if (activityInstanceKey > 0)
            {
                final WorkflowInstance workflowInstance = workflowInstanceIndex.get(taskHeaders.getWorkflowInstanceKey());

                isActive = workflowInstance != null && activityInstanceKey == workflowInstance.getActivityInstanceKey();
            }
        }

        @Override
        public void updateState(TypedRecord<TaskRecord> record)
        {
            if (isActive)
            {
                final long activityInstanceKey = record.getValue()
                    .headers()
                    .getActivityInstanceKey();

                activityInstanceMap
                    .wrapActivityInstanceKey(activityInstanceKey)
                    .setTaskKey(record.getKey())
                    .write();
            }
        }
    }

    private final class TaskCompletedEventProcessor implements TypedRecordProcessor<TaskRecord>
    {
        private final WorkflowInstanceRecord workflowInstanceEvent = new WorkflowInstanceRecord();

        private boolean activityCompleted;
        private long activityInstanceKey;

        @Override
        public void processRecord(TypedRecord<TaskRecord> record)
        {
            activityCompleted = false;

            final TaskRecord taskEvent = record.getValue();
            final TaskHeaders taskHeaders = taskEvent.headers();
            activityInstanceKey = taskHeaders.getActivityInstanceKey();

            if (taskHeaders.getWorkflowInstanceKey() > 0 && isTaskOpen(record.getKey(), activityInstanceKey))
            {
                workflowInstanceEvent
                    .setBpmnProcessId(taskHeaders.getBpmnProcessId())
                    .setVersion(taskHeaders.getWorkflowDefinitionVersion())
                    .setWorkflowKey(taskHeaders.getWorkflowKey())
                    .setWorkflowInstanceKey(taskHeaders.getWorkflowInstanceKey())
                    .setActivityId(taskHeaders.getActivityId())
                    .setPayload(taskEvent.getPayload());

                activityCompleted = true;
            }
        }

        private boolean isTaskOpen(long taskKey, long activityInstanceKey)
        {
            // task key = -1 when activity is left
            return activityInstanceMap.wrapActivityInstanceKey(activityInstanceKey).getTaskKey() == taskKey;
        }

        @Override
        public long writeRecord(TypedRecord<TaskRecord> record, TypedStreamWriter writer)
        {
            return activityCompleted ?
                    writer.writeFollowUpEvent(activityInstanceKey, WorkflowInstanceIntent.ACTIVITY_COMPLETING, workflowInstanceEvent) :
                    0L;
        }

        @Override
        public void updateState(TypedRecord<TaskRecord> record)
        {
            if (activityCompleted)
            {
                activityInstanceMap
                    .wrapActivityInstanceKey(activityInstanceKey)
                    .setTaskKey(-1L)
                    .write();
            }
        }
    }

    private final class ActivityCompletingEventProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private final IncidentRecord incidentCommand = new IncidentRecord();
        private boolean hasIncident;
        private boolean isResolvingIncident;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> record)
        {
            hasIncident = false;
            isResolvingIncident = record.getMetadata().hasIncidentKey();

            final WorkflowInstanceRecord activityEvent = record.getValue();

            final ServiceTask serviceTask = getActivity(activityEvent.getWorkflowKey(), activityEvent.getActivityId());
            final Mapping[] outputMappings = serviceTask.getInputOutputMapping().getOutputMappings();

            final DirectBuffer workflowInstancePayload = payloadCache.getPayload(activityEvent.getWorkflowInstanceKey());
            final DirectBuffer taskPayload = activityEvent.getPayload();
            final boolean isNilPayload = isNilPayload(taskPayload);

            if (outputMappings.length > 0)
            {
                if (isNilPayload)
                {
                    incidentCommand.reset();
                    incidentCommand
                        .initFromWorkflowInstanceFailure(record)
                        .setErrorType(ErrorType.IO_MAPPING_ERROR)
                        .setErrorMessage("Could not apply output mappings: Task was completed without payload");

                    hasIncident = true;
                }
                else
                {
                    try
                    {
                        final int resultLen = payloadMappingProcessor.merge(taskPayload, workflowInstancePayload, outputMappings);
                        final MutableDirectBuffer mergedPayload = payloadMappingProcessor.getResultBuffer();
                        activityEvent.setPayload(mergedPayload, 0, resultLen);
                    }
                    catch (MappingException e)
                    {
                        incidentCommand.reset();
                        incidentCommand
                            .initFromWorkflowInstanceFailure(record)
                            .setErrorType(ErrorType.IO_MAPPING_ERROR)
                            .setErrorMessage(e.getMessage());

                        hasIncident = true;
                    }
                }
            }
            else if (isNilPayload)
            {
                // no payload from task complete
                activityEvent.setPayload(workflowInstancePayload);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> record, TypedStreamWriter writer)
        {
            if (!hasIncident)
            {
                return writer.writeFollowUpEvent(record.getKey(), WorkflowInstanceIntent.ACTIVITY_COMPLETED, record.getValue());
            }
            else
            {
                if (!isResolvingIncident)
                {
                    return writer.writeNewCommand(IncidentIntent.CREATE, incidentCommand);
                }
                else
                {
                    return writer.writeFollowUpEvent(record.getMetadata().getIncidentKey(), IncidentIntent.RESOLVE_FAILED, incidentCommand);
                }
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> record)
        {
            if (!hasIncident)
            {
                workflowInstanceIndex
                    .get(record.getValue().getWorkflowInstanceKey())
                    .setActivityInstanceKey(-1L)
                    .write();

                activityInstanceMap.remove(record.getKey());
            }
        }
    }

    private final class CancelWorkflowInstanceProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private final WorkflowInstanceRecord activityInstanceEvent = new WorkflowInstanceRecord();
        private final TaskRecord taskEvent = new TaskRecord();

        private boolean isCanceled;
        private long activityInstanceKey;
        private long taskKey;

        private TypedStreamReader reader;
        private TypedRecord<WorkflowInstanceRecord> workflowInstanceEvent;

        @Override
        public void onOpen(TypedStreamProcessor streamProcessor)
        {
            reader = streamProcessor.getEnvironment().buildStreamReader();
        }

        @Override
        public void onClose()
        {
            reader.close();
        }

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> command)
        {
            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(command.getKey());

            isCanceled = workflowInstance != null && workflowInstance.getTokenCount() > 0;

            if (isCanceled)
            {
                workflowInstanceEvent = reader.readValue(workflowInstance.getPosition(), WorkflowInstanceRecord.class);

                workflowInstanceEvent.getValue().setPayload(WorkflowInstanceRecord.NO_PAYLOAD);

                activityInstanceKey = workflowInstance.getActivityInstanceKey();
                taskKey = activityInstanceMap.wrapActivityInstanceKey(activityInstanceKey).getTaskKey();
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> command, TypedStreamWriter writer)
        {
            if (isCanceled)
            {
                activityInstanceMap.wrapActivityInstanceKey(activityInstanceKey);
                final WorkflowInstanceRecord value = workflowInstanceEvent.getValue();

                final TypedBatchWriter batchWriter = writer.newBatch();

                if (taskKey > 0)
                {
                    taskEvent.reset();
                    taskEvent
                        .setType(EMPTY_TASK_TYPE)
                        .headers()
                            .setBpmnProcessId(value.getBpmnProcessId())
                            .setWorkflowDefinitionVersion(value.getVersion())
                            .setWorkflowInstanceKey(command.getKey())
                            .setActivityId(activityInstanceMap.getActivityId())
                            .setActivityInstanceKey(activityInstanceKey);

                    batchWriter.addFollowUpCommand(taskKey, TaskIntent.CANCEL, taskEvent);
                }

                if (activityInstanceKey > 0)
                {
                    activityInstanceEvent.reset();
                    activityInstanceEvent
                        .setBpmnProcessId(value.getBpmnProcessId())
                        .setVersion(value.getVersion())
                        .setWorkflowInstanceKey(command.getKey())
                        .setActivityId(activityInstanceMap.getActivityId());

                    batchWriter.addFollowUpEvent(activityInstanceKey, WorkflowInstanceIntent.ACTIVITY_TERMINATED, activityInstanceEvent);
                }

                batchWriter.addFollowUpEvent(command.getKey(), WorkflowInstanceIntent.CANCELED, value);

                return batchWriter.write();
            }
            else
            {
                return writer.writeRejection(command);
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowInstanceRecord> record, TypedResponseWriter responseWriter)
        {
            if (isCanceled)
            {
                return responseWriter.writeEvent(WorkflowInstanceIntent.CANCELED, record);
            }
            else
            {
                return responseWriter.writeRejection(record);
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> record)
        {
            if (isCanceled)
            {
                workflowInstanceIndex.remove(record.getKey());
                payloadCache.remove(record.getKey());
                activityInstanceMap.remove(activityInstanceKey);
            }
        }
    }

    private final class UpdatePayloadProcessor implements TypedRecordProcessor<WorkflowInstanceRecord>
    {
        private boolean isUpdated;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceRecord> command)
        {
            final WorkflowInstanceRecord workflowInstanceEvent = command.getValue();

            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(workflowInstanceEvent.getWorkflowInstanceKey());
            final boolean isActive = workflowInstance != null && workflowInstance.getTokenCount() > 0;

            isUpdated = isActive && isValidPayload(workflowInstanceEvent.getPayload());
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowInstanceRecord> command, TypedResponseWriter responseWriter)
        {
            if (isUpdated)
            {
                return responseWriter.writeEvent(WorkflowInstanceIntent.PAYLOAD_UPDATED, command);
            }
            else
            {
                return responseWriter.writeRejection(command);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceRecord> command, TypedStreamWriter writer)
        {
            if (isUpdated)
            {
                return writer.writeFollowUpEvent(command.getKey(), WorkflowInstanceIntent.PAYLOAD_UPDATED, command.getValue());
            }
            else
            {
                return writer.writeRejection(command);
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceRecord> command)
        {
            if (isUpdated)
            {
                final WorkflowInstanceRecord workflowInstanceEvent = command.getValue();
                payloadCache.addPayload(workflowInstanceEvent.getWorkflowInstanceKey(), command.getPosition(), workflowInstanceEvent.getPayload());
            }
        }
    }
}
