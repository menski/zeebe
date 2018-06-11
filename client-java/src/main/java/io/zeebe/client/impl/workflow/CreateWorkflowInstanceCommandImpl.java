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
package io.zeebe.client.impl.workflow;

import io.zeebe.client.api.commands.CreateWorkflowInstanceCommandStep1;
import io.zeebe.client.api.commands.CreateWorkflowInstanceCommandStep1.CreateWorkflowInstanceCommandStep2;
import io.zeebe.client.api.commands.CreateWorkflowInstanceCommandStep1.CreateWorkflowInstanceCommandStep3;
import io.zeebe.client.api.events.WorkflowInstanceEvent;
import io.zeebe.client.impl.CommandImpl;
import io.zeebe.client.impl.RequestManager;
import io.zeebe.client.impl.command.WorkflowInstanceCommandImpl;
import io.zeebe.client.impl.data.ZeebeObjectMapperImpl;
import io.zeebe.client.impl.record.RecordImpl;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import java.io.InputStream;

public class CreateWorkflowInstanceCommandImpl extends CommandImpl<WorkflowInstanceEvent>
    implements CreateWorkflowInstanceCommandStep1,
        CreateWorkflowInstanceCommandStep2,
        CreateWorkflowInstanceCommandStep3 {
  private final WorkflowInstanceCommandImpl command;

  public CreateWorkflowInstanceCommandImpl(
      final RequestManager commandManager, ZeebeObjectMapperImpl objectMapper, String topic) {
    super(commandManager);

    command = new WorkflowInstanceCommandImpl(objectMapper, WorkflowInstanceIntent.CREATE);
    command.setTopicName(topic);
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 payload(final InputStream payload) {
    this.command.setPayload(payload);
    return this;
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 payload(final String payload) {
    this.command.setPayload(payload);
    return this;
  }

  @Override
  public CreateWorkflowInstanceCommandStep2 bpmnProcessId(final String id) {
    this.command.setBpmnProcessId(id);
    return this;
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 version(final int version) {
    this.command.setVersion(version);
    return this;
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 latestVersion() {
    return version(LATEST_VERSION);
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 latestVersionForce() {
    return version(FORCE_LATEST_VERSION);
  }

  @Override
  public CreateWorkflowInstanceCommandStep3 workflowKey(long workflowKey) {
    this.command.setWorkflowKey(workflowKey);
    return this;
  }

  @Override
  public RecordImpl getCommand() {
    return command;
  }
}
