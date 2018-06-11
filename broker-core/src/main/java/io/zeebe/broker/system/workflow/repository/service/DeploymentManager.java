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
package io.zeebe.broker.system.workflow.repository.service;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.SystemServiceNames;
import io.zeebe.broker.system.workflow.repository.api.client.GetWorkflowControlMessageHandler;
import io.zeebe.broker.system.workflow.repository.api.client.ListWorkflowsControlMessageHandler;
import io.zeebe.broker.system.workflow.repository.api.management.DeploymentManagerRequestHandler;
import io.zeebe.broker.system.workflow.repository.api.management.FetchWorkflowRequestHandler;
import io.zeebe.broker.system.workflow.repository.processor.*;
import io.zeebe.broker.system.workflow.repository.processor.state.WorkflowRepositoryIndex;
import io.zeebe.broker.transport.controlmessage.ControlMessageHandlerManager;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TopicIntent;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ServerTransport;

public class DeploymentManager implements Service<DeploymentManager> {
  private final ServiceGroupReference<Partition> partitionsGroupReference =
      ServiceGroupReference.<Partition>create()
          .onAdd((name, partition) -> installServices(partition, name))
          .build();

  private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector =
      new Injector<>();
  private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
  private final Injector<DeploymentManagerRequestHandler> requestHandlerServiceInjector =
      new Injector<>();
  private final Injector<ControlMessageHandlerManager> controlMessageHandlerManagerServiceInjector =
      new Injector<>();

  private ServerTransport clientApiTransport;
  private StreamProcessorServiceFactory streamProcessorServiceFactory;

  private DeploymentManagerRequestHandler requestHandlerService;

  private ServiceStartContext startContext;

  private GetWorkflowControlMessageHandler getWorkflowMessageHandler;
  private ListWorkflowsControlMessageHandler listWorkflowsControlMessageHandler;

  @Override
  public void start(ServiceStartContext startContext) {
    this.startContext = startContext;
    this.clientApiTransport = clientApiTransportInjector.getValue();
    this.streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
    this.requestHandlerService = requestHandlerServiceInjector.getValue();

    getWorkflowMessageHandler =
        new GetWorkflowControlMessageHandler(clientApiTransport.getOutput());
    listWorkflowsControlMessageHandler =
        new ListWorkflowsControlMessageHandler(clientApiTransport.getOutput());

    final ControlMessageHandlerManager controlMessageHandlerManager =
        controlMessageHandlerManagerServiceInjector.getValue();
    controlMessageHandlerManager.registerHandler(getWorkflowMessageHandler);
    controlMessageHandlerManager.registerHandler(listWorkflowsControlMessageHandler);
  }

  private void installServices(
      final Partition partition, ServiceName<Partition> partitionServiceName) {
    final TypedStreamEnvironment streamEnvironment =
        new TypedStreamEnvironment(partition.getLogStream(), clientApiTransport.getOutput());

    final WorkflowRepositoryIndex repositoryIndex = new WorkflowRepositoryIndex();

    final TypedStreamProcessor streamProcessor =
        streamEnvironment
            .newStreamProcessor()
            .onCommand(
                ValueType.DEPLOYMENT,
                DeploymentIntent.CREATE,
                new DeploymentCreateEventProcessor(repositoryIndex))
            .onEvent(
                ValueType.DEPLOYMENT,
                DeploymentIntent.CREATED,
                new DeploymentCreatedEventProcessor(repositoryIndex))
            .onRejection(
                ValueType.DEPLOYMENT,
                DeploymentIntent.CREATE,
                new DeploymentRejectedEventProcessor())
            .onEvent(
                ValueType.TOPIC,
                TopicIntent.CREATING,
                new DeploymentTopicCreatingEventProcessor(repositoryIndex))
            .withStateResource(repositoryIndex)
            .withListener(
                new StreamProcessorLifecycleAware() {
                  private BufferedLogStreamReader reader;

                  @Override
                  public void onOpen(TypedStreamProcessor streamProcessor) {
                    final StreamProcessorContext ctx = streamProcessor.getStreamProcessorContext();

                    reader = new BufferedLogStreamReader();
                    reader.wrap(ctx.getLogStream());

                    final DeploymentResourceCache cache = new DeploymentResourceCache(reader);

                    final WorkflowRepositoryService workflowRepositoryService =
                        new WorkflowRepositoryService(
                            ctx.getActorControl(), repositoryIndex, cache);

                    startContext
                        .createService(
                            SystemServiceNames.REPOSITORY_SERVICE, workflowRepositoryService)
                        .dependency(partitionServiceName)
                        .install();

                    final FetchWorkflowRequestHandler requestHandler =
                        new FetchWorkflowRequestHandler(workflowRepositoryService);
                    requestHandlerService.setFetchWorkflowRequestHandler(requestHandler);

                    getWorkflowMessageHandler.setWorkflowRepositoryService(
                        workflowRepositoryService);
                    listWorkflowsControlMessageHandler.setWorkflowRepositoryService(
                        workflowRepositoryService);
                  }

                  @Override
                  public void onClose() {
                    requestHandlerService.setFetchWorkflowRequestHandler(null);
                    getWorkflowMessageHandler.setWorkflowRepositoryService(null);
                    listWorkflowsControlMessageHandler.setWorkflowRepositoryService(null);

                    reader.close();
                  }
                })
            .build();

    streamProcessorServiceFactory
        .createService(partition, partitionServiceName)
        .processor(streamProcessor)
        .processorId(StreamProcessorIds.DEPLOYMENT_PROCESSOR_ID)
        .processorName("deployment")
        .build();
  }

  @Override
  public DeploymentManager get() {
    return this;
  }

  public ServiceGroupReference<Partition> getPartitionsGroupReference() {
    return partitionsGroupReference;
  }

  public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector() {
    return streamProcessorServiceFactoryInjector;
  }

  public Injector<ServerTransport> getClientApiTransportInjector() {
    return clientApiTransportInjector;
  }

  public Injector<DeploymentManagerRequestHandler> getRequestHandlerServiceInjector() {
    return requestHandlerServiceInjector;
  }

  public Injector<ControlMessageHandlerManager> getControlMessageHandlerManagerServiceInjector() {
    return controlMessageHandlerManagerServiceInjector;
  }
}
