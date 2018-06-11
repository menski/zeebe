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
package io.zeebe.broker.it.workflow;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.broker.it.util.TopicEventRecorder;
import io.zeebe.client.api.commands.DeployedWorkflow;
import io.zeebe.client.api.commands.DeploymentResource;
import io.zeebe.client.api.commands.ResourceType;
import io.zeebe.client.api.commands.Workflow;
import io.zeebe.client.api.events.DeploymentEvent;
import io.zeebe.client.cmd.ClientCommandRejectedException;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.impl.instance.ProcessImpl;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class CreateDeploymentTest {
  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientRule clientRule = new ClientRule();
  public TopicEventRecorder eventRecorder = new TopicEventRecorder(clientRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(brokerRule).around(clientRule).around(eventRecorder);

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void shouldDeployWorkflowModel() {
    // given
    final WorkflowDefinition workflow =
        Bpmn.createExecutableWorkflow("process").startEvent().endEvent().done();
    // when
    final DeploymentEvent result =
        clientRule
            .getWorkflowClient()
            .newDeployCommand()
            .addWorkflowModel(workflow, "workflow.bpmn")
            .send()
            .join();

    // then
    assertThat(result.getMetadata().getKey()).isGreaterThan(0);
    assertThat(result.getResources()).hasSize(1);

    final DeploymentResource deployedResource = result.getResources().get(0);
    assertThat(deployedResource.getResource())
        .isEqualTo(Bpmn.convertToString(workflow).getBytes(UTF_8));
    assertThat(deployedResource.getResourceType()).isEqualTo(ResourceType.BPMN_XML);
    assertThat(deployedResource.getResourceName()).isEqualTo("workflow.bpmn");

    assertThat(result.getDeployedWorkflows()).hasSize(1);

    final DeployedWorkflow deployedWorkflow = result.getDeployedWorkflows().get(0);
    assertThat(deployedWorkflow.getBpmnProcessId()).isEqualTo("process");
    assertThat(deployedWorkflow.getVersion()).isEqualTo(1);
    assertThat(deployedWorkflow.getWorkflowKey()).isEqualTo(1L);
    assertThat(deployedWorkflow.getResourceName()).isEqualTo("workflow.bpmn");
  }

  @Test
  public void shouldNotDeployUnparsableModel() {
    // then
    exception.expect(ClientCommandRejectedException.class);
    exception.expectMessage("Failed to deploy resource 'invalid.bpmn'");

    // when
    clientRule
        .getWorkflowClient()
        .newDeployCommand()
        .addResourceStringUtf8("Foooo", "invalid.bpmn")
        .send()
        .join();
  }

  @Test
  public void shouldNotDeployInvalidModel() throws Exception {
    // then
    exception.expect(ClientCommandRejectedException.class);
    exception.expectMessage("The process must contain at least one none start event.");

    // when
    clientRule
        .getWorkflowClient()
        .newDeployCommand()
        .addResourceFile(
            getClass()
                .getResource("/workflows/invalid_process.bpmn")
                .getFile()) // does not have a start event
        .send()
        .join();
  }

  @Test
  public void shouldNotDeployNonExecutableModel() {
    // given
    final WorkflowDefinition model =
        Bpmn.createExecutableWorkflow("not-executable").startEvent().endEvent().done();

    final ProcessImpl workflowImpl = (ProcessImpl) model.getWorkflows().iterator().next();
    workflowImpl.setExecutable(false);

    // then
    exception.expect(ClientCommandRejectedException.class);
    exception.expectMessage("BPMN model must contain at least one executable process");

    // when
    clientRule
        .getWorkflowClient()
        .newDeployCommand()
        .addWorkflowModel(model, "workflow.bpmn")
        .send()
        .join();
  }

  @Test
  public void shouldDeployYamlWorkflow() {
    // when
    final DeploymentEvent result =
        clientRule
            .getWorkflowClient()
            .newDeployCommand()
            .addResourceFromClasspath("workflows/simple-workflow.yaml")
            .send()
            .join();

    // then
    assertThat(result.getMetadata().getKey()).isGreaterThan(0);

    assertThat(result.getResources()).hasSize(1);

    final DeploymentResource deployedResource = result.getResources().get(0);
    assertThat(deployedResource.getResourceType()).isEqualTo(ResourceType.YAML_WORKFLOW);
    assertThat(deployedResource.getResourceName()).isEqualTo("workflows/simple-workflow.yaml");

    assertThat(result.getDeployedWorkflows()).hasSize(1);

    final Workflow deployedWorkflow = result.getDeployedWorkflows().get(0);
    assertThat(deployedWorkflow.getBpmnProcessId()).isEqualTo("yaml-workflow");
    assertThat(deployedWorkflow.getVersion()).isEqualTo(1);
    assertThat(deployedWorkflow.getWorkflowKey()).isGreaterThan(0);
  }
}
