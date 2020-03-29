package io.atomix.core;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.utils.net.Address;
import io.zeebe.test.util.socket.SocketUtil;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class AtomixRule extends ExternalResource {
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File dataDir;
  private Map<Integer, Address> addressMap;

  @Override
  public Statement apply(final Statement base, final Description description) {
    return temporaryFolder.apply(super.apply(base, description), description);
  }

  public void before() throws IOException {
    dataDir = temporaryFolder.newFolder();
    addressMap = new HashMap<>();
  }

  public File getDataDir() {
    return dataDir;
  }

  /** Creates an Atomix instance. */
  public AtomixBuilder buildAtomix(
      final int id, final List<Integer> memberIds, final Properties properties) {
    final Collection<Node> nodes =
        memberIds.stream()
            .map(
                memberId -> {
                  final var address = getAddress(memberId);

                  return Node.builder()
                      .withId(String.valueOf(memberId))
                      .withAddress(address)
                      .build();
                })
            .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(id))
        .withHost("localhost")
        .withPort(getAddress(id).port())
        .withProperties(properties)
        .withMulticastEnabled()
        .withMembershipProvider(
            !nodes.isEmpty()
                ? new BootstrapDiscoveryProvider(nodes)
                : new MulticastDiscoveryProvider());
  }

  private Address getAddress(final Integer memberId) {
    return addressMap.computeIfAbsent(
        memberId,
        newId -> {
          final var nextInetAddress = SocketUtil.getNextAddress();
          final var addressString = io.zeebe.util.SocketUtil.toHostAndPortString(nextInetAddress);
          return Address.from(addressString);
        });
  }

  /** Creates an Atomix instance. */
  public Atomix createAtomix(
      final int id,
      final List<Integer> bootstrapIds,
      final Function<AtomixBuilder, Atomix> builderFunction) {
    return createAtomix(id, bootstrapIds, new Properties(), builderFunction);
  }

  /** Creates an Atomix instance. */
  public Atomix createAtomix(
      final int id,
      final List<Integer> bootstrapIds,
      final Properties properties,
      final Function<AtomixBuilder, Atomix> builderFunction) {
    return builderFunction.apply(buildAtomix(id, bootstrapIds, properties));
  }
}
