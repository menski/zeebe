package io.zeebe.broker.clustering.orchestration.topic;

import java.time.Duration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.ListSnapshotsRequest;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
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
                actor.runDelayed(Duration.ofMillis(10), this::pollLeaderForSnapshots);
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

        actor.runOnCompletion(responseFuture, (createPartitionResponse, error) ->
        {
            if (error != null)
            {
                LOG.error("failed to list snapshots", error);
                actor.runDelayed(Duration.ofMillis(10), this::pollLeaderForSnapshots);
            }
            else
            {
                
            }
        });
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
