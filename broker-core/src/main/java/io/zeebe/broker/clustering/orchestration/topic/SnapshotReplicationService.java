package io.zeebe.broker.clustering.orchestration.topic;

import java.io.IOException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.*;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.clustering.management.ErrorResponseDecoder;
import io.zeebe.clustering.management.FetchSnapshotChunkResponseDecoder;
import io.zeebe.clustering.management.ListSnapshotsResponseDecoder;
import io.zeebe.clustering.management.MessageHeaderDecoder;
import io.zeebe.logstreams.spi.SnapshotWriter;
import io.zeebe.logstreams.spi.TemporarySnapshotWriter;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransportBuilder;
import io.zeebe.util.LangUtil;
import io.zeebe.util.buffer.DirectBufferOutputStream;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class SnapshotReplicationService extends Actor implements Service<SnapshotReplicationService>
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    private final Injector<ClientTransport> managementClientApiInjector = new Injector<>();
    private ClientTransport clientTransport;

    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
    private TopologyManager topologyManager;

    private final Injector<Partition> partitionInjector = new Injector<>();
    private Partition partition;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ErrorResponse errorResponse = new ErrorResponse();

    @Override
    public void start(ServiceStartContext startContext)
    {
        clientTransport = managementClientApiInjector.getValue();
        partition = partitionInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();

        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(actor.close());
    }

    @Override
    public SnapshotReplicationService get()
    {
        return this;
    }

    @Override
    protected void onActorStarted()
    {
        actor.run(this::pollLeaderForSnapshots);
    }

    private void pollLeaderForSnapshots()
    {
        final ActorFuture<NodeInfo> topologyQuery = topologyManager.query(this::getLeaderInfo);
        actor.runOnCompletion(topologyQuery, (leaderInfo, error) ->
        {
            if (error != null)
            {
                LOG.error("failed to query topology for leader info", error);
                actor.runDelayed(Duration.ofMillis(1000), this::pollLeaderForSnapshots);
            }
            else if (leaderInfo == null)
            {
                LOG.debug("waiting for leader node info");
                actor.runDelayed(Duration.ofMillis(1000), this::pollLeaderForSnapshots);
            }
            else
            {
                pollSnapshots(leaderInfo);
            }
        });
    }

    private void pollSnapshots(NodeInfo leaderInfo)
    {
        final ListSnapshotsRequest request = new ListSnapshotsRequest().setPartitionId(partition.getInfo().getPartitionId());
        final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(leaderInfo.getManagementApiAddress());
        final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

        actor.runOnCompletion(responseFuture, (clientResponse, error) ->
        {
            if (error != null)
            {
                LOG.error("failed to list snapshots", error);
                actor.runDelayed(Duration.ofMillis(10), this::pollLeaderForSnapshots);
            }
            else
            {
                final DirectBuffer buffer = clientResponse.getResponseBuffer();
                if (isExpectedResponse(buffer, ListSnapshotsResponseDecoder.TEMPLATE_ID))
                {
                    final ListSnapshotsResponse response = new ListSnapshotsResponse();
                    response.wrap(buffer, 0, buffer.capacity());

                    List<ActorFuture<Void>> fetchOperations = new ArrayList<>();
                    for (ListSnapshotsResponse.SnapshotMetadata snapshot : response.getSnapshots())
                    {
                        TemporarySnapshotWriter writer;

                        try
                        {
                            writer = partition.getSnapshotStorage().createTemporarySnapshot(snapshot.getName(), snapshot.getLogPosition());
                        }
                        catch (final Exception ex)
                        {
                            LOG.error("could not create snapshot, skipping", ex);
                            return;
                        }

                        final SnapshotFetcher fetcher = new SnapshotFetcher(snapshot, remoteAddress, writer);
                        fetchOperations.add(fetcher.fetch());
                    }

                    actor.runOnCompletion(fetchOperations, (t) -> pollLeaderForSnapshots());
                }
            }
        });
    }

    private boolean isExpectedResponse(final DirectBuffer responseBuffer, final int expectedTemplateId)
    {
        messageHeaderDecoder.wrap(responseBuffer, 0);

        if (messageHeaderDecoder.templateId() == expectedTemplateId)
        {
            return true;
        }

        if (messageHeaderDecoder.templateId() == ErrorResponseDecoder.TEMPLATE_ID)
        {
            handleError(responseBuffer);
        }
        else
        {
            LOG.error("unexpected response type {}", messageHeaderDecoder.templateId());
        }

        return false;
    }

    private void handleError(final DirectBuffer buffer)
    {
        errorResponse.wrap(buffer, 0, buffer.capacity());

    }

    private NodeInfo getLeaderInfo(ReadableTopology topology)
    {
        return topology.getLeader(partition.getInfo().getPartitionId());
    }

    public Injector<ClientTransport> getManagementClientApiInjector()
    {
        return managementClientApiInjector;
    }

    public Injector<Partition> getPartitionInjector()
    {
        return partitionInjector;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }

    class SnapshotFetcher
    {
        private CompletableActorFuture<Void> awaitFetch;
        private final ListSnapshotsResponse.SnapshotMetadata snapshotMetadata;
        private final RemoteAddress sourceAddress;
        private final TemporarySnapshotWriter snapshotWriter;

        private final FetchSnapshotChunkRequest request = new FetchSnapshotChunkRequest();
        private final FetchSnapshotChunkResponse response = new FetchSnapshotChunkResponse();

        private int chunkOffset = 0;

        SnapshotFetcher(final ListSnapshotsResponse.SnapshotMetadata snapshotMetadata, final RemoteAddress sourceAddress, final TemporarySnapshotWriter writer)
        {
            this.snapshotMetadata = snapshotMetadata;
            this.sourceAddress = sourceAddress;
            this.snapshotWriter = writer;

            this.request.setChunkOffset(0)
                    .setChunkLength(ServerTransportBuilder.DEFAULT_MAX_MESSAGE_LENGTH)
                    .setPartitionId(partition.getInfo().getPartitionId())
                    .setName(snapshotMetadata.getName())
                    .setLogPosition(snapshotMetadata.getLogPosition());
        }

        ActorFuture<Void> fetch()
        {
            awaitFetch = new CompletableActorFuture<>();
            fetchNextChunk();
            return awaitFetch;
        }

        void fetchNextChunk()
        {
            final ActorFuture<ClientResponse> awaitRequest = clientTransport.getOutput().sendRequest(sourceAddress, request.setChunkOffset(chunkOffset));
            actor.runOnCompletion(awaitRequest, (clientResponse, error) ->
            {
                if (error != null)
                {
                    abort(error);
                    return;
                }

                final DirectBuffer buffer = clientResponse.getResponseBuffer();
                if (isExpectedResponse(buffer, FetchSnapshotChunkResponseDecoder.TEMPLATE_ID))
                {
                    response.wrap(buffer, 0, buffer.capacity());

                    try
                    {
                        snapshotWriter.write(response.getData(), 0, response.getData().capacity());
                    }
                    catch (final Exception ex)
                    {
                        abort(ex);
                        return;
                    }

                    chunkOffset += request.getChunkLength();
                    if (chunkOffset >= snapshotMetadata.getLength())
                    {
                        try
                        {
                            snapshotWriter.commit(snapshotMetadata.getChecksum());
                            awaitFetch.complete(null);
                        }
                        catch (final Exception ex)
                        {
                            abort(ex);
                        }
                    }
                    else
                    {
                        fetchNextChunk();
                    }
                }
                else
                {
                    awaitFetch.completeExceptionally(new IllegalStateException("blah"));
                }
            });
        }

        void abort(Throwable error)
        {
            LOG.error("could not fetch snapshot", error);
            snapshotWriter.abort();
            awaitFetch.completeExceptionally(error);
        }
    }
}
