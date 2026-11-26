package dev.francisworld.fraudstream.support;

import dev.francisworld.fraudstream.query.StateAccess;
import java.util.List;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/** StateAccess over a test driver, with a configurable view of where keys live. */
public final class DriverStateAccess implements StateAccess {

  private final PipelineFixture fixture;
  private final Placement placement;
  private final List<Peer> peers;

  public DriverStateAccess(PipelineFixture fixture, Placement placement, List<Peer> peers) {
    this.fixture = fixture;
    this.placement = placement;
    this.peers = peers;
  }

  public static DriverStateAccess local(PipelineFixture fixture) {
    return new DriverStateAccess(fixture, new Placement.Local(), List.of());
  }

  @Override
  public Placement locate(String store, String key) {
    return placement;
  }

  @Override
  public <V> ReadOnlyKeyValueStore<String, V> store(String name) {
    return fixture.store(name);
  }

  @Override
  public List<Peer> peers() {
    return peers;
  }
}
