package dev.francisworld.fraudstream;

import dev.francisworld.fraudstream.config.AppConfig;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service entry point. All configuration comes from FRAUDSTREAM_* environment variables. */
public final class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private Main() {}

  public static void main(String[] args) throws Exception {
    AppConfig config;
    try {
      config = AppConfig.fromEnv(System.getenv());
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid configuration: {}", e.getMessage());
      System.exit(2);
      return;
    }
    Application app;
    try {
      app = Application.start(config);
    } catch (IllegalArgumentException e) {
      LOG.error("Cannot start: {}", e.getMessage());
      System.exit(2);
      return;
    }
    CountDownLatch stopped = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  app.close();
                  stopped.countDown();
                },
                "shutdown"));
    LOG.info("fraudstream started");
    stopped.await();
  }
}
