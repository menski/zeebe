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
package io.zeebe.broker.system.deployment.processor;

import static io.zeebe.util.EnsureUtil.ensureGreaterThan;
import static io.zeebe.util.EnsureUtil.ensureNotNull;

import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.deployment.data.*;
import io.zeebe.broker.system.deployment.data.PendingDeployments.PendingDeployment;
import io.zeebe.broker.system.deployment.data.TopicPartitions.TopicPartition;
import io.zeebe.broker.system.deployment.data.TopicPartitions.TopicPartitionIterator;
import io.zeebe.broker.system.deployment.handler.RemoteWorkflowsManager;
import io.zeebe.broker.workflow.data.WorkflowRecord;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;

public class WorkflowCreateProcessor implements TypedRecordProcessor<WorkflowRecord>
{
    private final TopicPartitions topicPartitions;
    private final PendingDeployments pendingDeployments;
    private final PendingWorkflows pendingWorkflows;

    private final RemoteWorkflowsManager workflowRequestSender;

    private final IntArrayList partitionIds = new IntArrayList();

    public WorkflowCreateProcessor(
            TopicPartitions topicPartitions,
            PendingDeployments pendingDeployments,
            PendingWorkflows pendingWorkflows,
            RemoteWorkflowsManager workflowRequestSender)
    {
        this.topicPartitions = topicPartitions;
        this.pendingDeployments = pendingDeployments;
        this.pendingWorkflows = pendingWorkflows;
        this.workflowRequestSender = workflowRequestSender;
    }

    @Override
    public void processRecord(TypedRecord<WorkflowRecord> command)
    {
        partitionIds.clear();

        final WorkflowRecord workflowEvent = command.getValue();

        final PendingDeployment pendingDeployment = pendingDeployments.get(workflowEvent.getDeploymentKey());
        ensureNotNull("pending deployment", pendingDeployment);

        final DirectBuffer topicName = pendingDeployment.getTopicName();

        final TopicPartitionIterator iterator = topicPartitions.iterator();
        while (iterator.hasNext())
        {
            final TopicPartition topicPartition = iterator.next();

            if (BufferUtil.equals(topicName, topicPartition.getTopicName()))
            {
                partitionIds.add(topicPartition.getPartitionId());
            }
        }

        ensureGreaterThan("partition ids", partitionIds.size(), 0);
    }

    @Override
    public boolean executeSideEffects(TypedRecord<WorkflowRecord> command, TypedResponseWriter responseWriter)
    {
        return workflowRequestSender.distributeWorkflow(
                   partitionIds,
                   command.getKey(),
                   command.getValue());
    }

    @Override
    public void updateState(TypedRecord<WorkflowRecord> command)
    {
        final WorkflowRecord workflowEvent = command.getValue();

        for (int partitionId: partitionIds)
        {
            pendingWorkflows.put(command.getKey(), partitionId, PendingWorkflows.STATE_CREATE, workflowEvent.getDeploymentKey());
        }
    }

}
