package dev.francisworld.fraudstream.config;

import dev.francisworld.fraudstream.topology.Topics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the application's own topics when asked to. Every topic that is co-partitioned in the
 * topology (transactions and case commands) gets the same partition count.
 */
public final class TopicProvisioner {

  private static final Logger LOG = LoggerFactory.getLogger(TopicProvisioner.class);

  private TopicProvisioner() {}

  public static List<NewTopic> plan(Topics topics, int partitions, short replication) {
    return List.of(
        new NewTopic(topics.transactions(), partitions, replication),
        new NewTopic(topics.caseCommands(), partitions, replication),
        new NewTopic(topics.alerts(), partitions, replication),
        new NewTopic(topics.caseEvents(), partitions, replication),
        new NewTopic(topics.lateEvents(), partitions, replication),
        new NewTopic(topics.deadLetters(), partitions, replication),
        // The rule set topic is read as a global table: one partition, compacted to the latest.
        new NewTopic(topics.rules(), 1, replication).configs(Map.of("cleanup.policy", "compact")));
  }

  public static void ensure(Admin admin, List<NewTopic> topics) throws InterruptedException {
    for (NewTopic topic : topics) {
      try {
        admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        LOG.info("Created topic {}", topic.name());
      } catch (ExecutionException e) {
        if (e.getCause() instanceof TopicExistsException) {
          LOG.debug("Topic {} already exists", topic.name());
        } else {
          throw new IllegalStateException("Cannot create topic " + topic.name(), e.getCause());
        }
      } catch (TimeoutException e) {
        throw new IllegalStateException("Timed out creating topic " + topic.name(), e);
      }
    }
  }
}
