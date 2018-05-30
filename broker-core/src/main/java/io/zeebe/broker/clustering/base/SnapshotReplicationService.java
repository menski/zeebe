package io.zeebe.broker.clustering.base;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.*;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.clustering.management.ErrorResponseDecoder;
import io.zeebe.clustering.management.MessageHeaderDecoder;
import io.zeebe.logstreams.spi.TemporarySnapshotWriter;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransportBuilder;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
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

    private final ListSnapshotsRequest listSnapshotsRequest = new ListSnapshotsRequest();
    private final ListSnapshotsResponse listSnapshotsResponse = new ListSnapshotsResponse();

    private final FetchSnapshotChunkRequest fetchSnapshotChunkRequest = new FetchSnapshotChunkRequest();
    private final FetchSnapshotChunkResponse fetchSnapshotChunkResponse = new FetchSnapshotChunkResponse();

    private final Queue<ListSnapshotsResponse.SnapshotMetadata> snapshotsToReplicate = new ArrayDeque<>();

    private RemoteAddress leaderNodeAddress;

    private final Duration pollInterval = Duration.ofSeconds(15);
    private final Duration topologyErrorRetryInterval = Duration.ofSeconds(1);
    private final Duration noLeaderRetryInterval = Duration.ofSeconds(5);
    private final Duration listErrorRetryInterval = Duration.ofSeconds(5);

    private TemporarySnapshotWriter temporarySnapshotWriter;
    private ListSnapshotsResponse.SnapshotMetadata snapshotMetadata;
    private int chunkOffset;

    @Override
    public void start(ServiceStartContext startContext)
    {
        clientTransport = managementClientApiInjector.getValue();
        partition = partitionInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();
        listSnapshotsRequest.setPartitionId(partition.getInfo().getPartitionId());

        LOG.debug("Starting snapshot replication service for partition {}", partition.getInfo());
        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        LOG.debug("Stopping snapshot replication service for partition {}", partition.getInfo());

        if (temporarySnapshotWriter != null)
        {
            temporarySnapshotWriter.abort();
        }

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
        this.pollLeaderForSnapshots();
    }

    private void pollLeaderForSnapshots()
    {
        final ActorFuture<NodeInfo> topologyQuery = topologyManager.query(this::getLeaderInfo);
        actor.runOnCompletion(topologyQuery, (leaderInfo, error) ->
        {
            if (error != null)
            {
                LOG.error("Failed to query topology for leader info, retrying", error);
                actor.runDelayed(topologyErrorRetryInterval, this::pollLeaderForSnapshots);
            }
            else if (leaderInfo == null)
            {
                LOG.debug("Waiting for leader node info, retrying");
                actor.runDelayed(noLeaderRetryInterval, this::pollLeaderForSnapshots);
            }
            else
            {
                leaderNodeAddress = clientTransport.registerRemoteAddress(leaderInfo.getManagementApiAddress());
                pollSnapshots();
            }
        });
    }

    private void pollSnapshots()
    {
        final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(leaderNodeAddress, listSnapshotsRequest);

        LOG.debug("Polling snapshots from {}", leaderNodeAddress);
        actor.runOnCompletion(responseFuture, (clientResponse, error) ->
        {
            if (error != null)
            {
                LOG.error("Error listing snapshots from leader", error);
                actor.runDelayed(listErrorRetryInterval, this::pollLeaderForSnapshots);
            }
            else
            {
                final DirectBuffer buffer = clientResponse.getResponseBuffer();

                if (isErrorResponse(buffer))
                {
                    logErrorResponse("Error listing snapshots for replication", buffer);
                    actor.runDelayed(listErrorRetryInterval, this::pollSnapshots);
                    return;
                }

                listSnapshotsResponse.wrap(buffer);
                snapshotsToReplicate.clear();

                for (ListSnapshotsResponse.SnapshotMetadata metadata : listSnapshotsResponse.getSnapshots())
                {
                    if (!partition.getSnapshotStorage().contains(metadata.getName(), metadata.getLogPosition()))
                    {
                        snapshotsToReplicate.add(metadata);
                    }
                }

                LOG.debug("Replicating {} snapshots", snapshotsToReplicate.size());
                replicateNextSnapshot();
            }
        });
    }

    private void replicateNextSnapshot()
    {
        chunkOffset = 0;
        snapshotMetadata = snapshotsToReplicate.poll();

        if (snapshotMetadata == null)
        {
            actor.runDelayed(pollInterval, this::pollSnapshots);
            return;
        }

        try
        {
            temporarySnapshotWriter = partition.getSnapshotStorage().createTemporarySnapshot(snapshotMetadata.getName(), snapshotMetadata.getLogPosition());
        }
        catch (final Exception ex)
        {
            LOG.error("Could not create temporary snapshot writer", ex);
            replicateNextSnapshot();
            return;
        }

        replicateSnapshot();
    }

    private void replicateSnapshot()
    {
        ActorFuture<ClientResponse> awaitFetchChunk = clientTransport.getOutput().sendRequest(leaderNodeAddress, requestForNextChunk());
        actor.runOnCompletion(awaitFetchChunk, (clientResponse, error) ->
        {
            if (error != null)
            {
                LOG.error("Error fetching snapshot chunk", error);
                abortCurrentSnapshotReplication();
                return;
            }

            final DirectBuffer buffer = clientResponse.getResponseBuffer();
            if (isErrorResponse(buffer))
            {
                logErrorResponse("Error fetching snapshot chunk", buffer);
                abortCurrentSnapshotReplication();
                return;
            }

            fetchSnapshotChunkResponse.wrap(buffer);
            final DirectBuffer chunk = fetchSnapshotChunkResponse.getData();
            try
            {
                temporarySnapshotWriter.write(chunk, 0, chunk.capacity());
            }
            catch (final Exception ex)
            {
                LOG.error("Error writing snapshot chunk", ex);
                abortCurrentSnapshotReplication();
                return;
            }

            chunkOffset += chunk.capacity();
            if (chunkOffset >= snapshotMetadata.getLength())
            {
                try
                {
                    temporarySnapshotWriter.commit(snapshotMetadata.getChecksum());
                }
                catch (final Exception ex)
                {
                    LOG.error("Error committing temporary snapshot", ex);
                    abortCurrentSnapshotReplication();
                    return;
                }

                replicateNextSnapshot();
            }
            else
            {
                replicateSnapshot();
            }
        });
    }

    private void abortCurrentSnapshotReplication()
    {
        chunkOffset = 0;
        snapshotMetadata = null;
        temporarySnapshotWriter.abort();

        this.replicateNextSnapshot();
    }

    private FetchSnapshotChunkRequest requestForNextChunk()
    {
        return fetchSnapshotChunkRequest.setPartitionId(partition.getInfo().getPartitionId())
                .setName(snapshotMetadata.getName())
                .setLogPosition(snapshotMetadata.getLogPosition())
                .setChunkLength(ServerTransportBuilder.DEFAULT_MAX_MESSAGE_LENGTH)
                .setChunkOffset(chunkOffset);
    }

    private void logErrorResponse(final String message, final DirectBuffer buffer)
    {
        errorResponse.wrap(buffer);
        LOG.error("{} - {} - {}", message, errorResponse.getCode(), errorResponse.getMessage());
    }

    private boolean isErrorResponse(final DirectBuffer buffer)
    {
        messageHeaderDecoder.wrap(buffer, 0);
        return messageHeaderDecoder.templateId() == ErrorResponseDecoder.TEMPLATE_ID;
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
}
