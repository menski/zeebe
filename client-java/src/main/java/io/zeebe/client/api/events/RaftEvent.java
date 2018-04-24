package io.zeebe.client.api.events;

import java.util.List;

import io.zeebe.client.api.record.Record;
import io.zeebe.transport.SocketAddress;

public interface RaftEvent extends Record
{
    /**
     * @return the list of members, can be null
     */
    List<SocketAddress> getMembers();
}
