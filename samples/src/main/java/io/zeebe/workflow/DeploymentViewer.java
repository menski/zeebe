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
package io.zeebe.workflow;

import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.clients.WorkflowClient;
import io.zeebe.client.api.commands.WorkflowResource;
import io.zeebe.client.api.commands.Workflows;

public class DeploymentViewer {

  public static void main(final String[] args) {
    final ZeebeClient client = ZeebeClient.newClientBuilder().build();

    final WorkflowClient workflowClient = client.topicClient().workflowClient();

    final Workflows workflows = workflowClient.newWorkflowRequest().send().join();

    workflows
        .getWorkflows()
        .forEach(
            wf -> {
              System.out.println("Fetching workflow resource for " + wf);

              final WorkflowResource resource =
                  workflowClient
                      .newResourceRequest()
                      .workflowKey(wf.getWorkflowKey())
                      .send()
                      .join();

              System.out.println(resource);
            });

    client.close();
  }
}
