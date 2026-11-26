package dev.francisworld.fraudstream.features;

import static org.assertj.core.api.Assertions.assertThat;

import dev.francisworld.fraudstream.model.DeviceLinks;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeviceGraphTest {

  private final DeviceGraph graph = new DeviceGraph(Duration.ofHours(24).toMillis());
  private final DeviceLinks empty = new DeviceLinks("dev-1", Map.of());

  @Test
  void tracksDistinctAccountsAndRefreshesLastSeen() {
    DeviceLinks links = graph.observe(empty, "a", 1_000);
    links = graph.observe(links, "b", 2_000);
    links = graph.observe(links, "a", 3_000);
    assertThat(links.accountLastSeenMs()).containsEntry("a", 3_000L).containsEntry("b", 2_000L);
  }

  @Test
  void lastSeenNeverMovesBackwardsOnLateEvents() {
    DeviceLinks links = graph.observe(empty, "a", 5_000);
    links = graph.observe(links, "a", 1_000);
    assertThat(links.accountLastSeenMs()).containsEntry("a", 5_000L);
  }

  @Test
  void accountsOutsideTheWindowAreForgotten() {
    long day = Duration.ofHours(24).toMillis();
    DeviceLinks links = graph.observe(empty, "old", 0);
    links = graph.observe(links, "new", day + 1);
    assertThat(links.accountLastSeenMs()).containsOnlyKeys("new");
  }

  @Test
  void theCurrentAccountIsKeptEvenWhenItsEventIsOlderThanTheWindow() {
    long day = Duration.ofHours(24).toMillis();
    DeviceLinks links = graph.observe(empty, "new", 2 * day);
    links = graph.observe(links, "late", 0);
    assertThat(links.accountLastSeenMs()).containsOnlyKeys("new", "late");
  }

  @Test
  void theGraphIsBounded() {
    DeviceLinks links = empty;
    for (int i = 0; i < DeviceGraph.MAX_ACCOUNTS + 10; i++) {
      links = graph.observe(links, "acc-" + i, i);
    }
    assertThat(links.accountLastSeenMs()).hasSize(DeviceGraph.MAX_ACCOUNTS);
    assertThat(links.accountLastSeenMs()).doesNotContainKey("acc-0");
  }
}
