/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.itest.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;
import org.diqube.config.ConfigKey;
import org.diqube.config.ConfigurationManager;
import org.diqube.itest.util.TestThriftConnectionFactory.TestConnection;
import org.diqube.itest.util.TestThriftConnectionFactory.TestConnectionException;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * Controls a single diqube-server
 *
 * @author Bastian Gloeckle
 */
public class ServerControl implements LogfileSaver {
  private static final Logger logger = LoggerFactory.getLogger(ServerControl.class);

  private static final String LOGBACK_DEFAULT_CONFIG_CLASSPATH = "/server/logback.default.xml";

  private File serverJarFile;
  private File workDir;
  /** The file containing the servers logback.xml configuration. */
  private File logbackConfig;
  /** File the server logs its log-output to. */
  private File serverLog;
  /** The server.properties the server uses. Valid after started. */
  private File serverPropertiesFile;

  private Process serverProcess = null;

  /** true if {@link #checkLivelinessThread} reported that the process died. */
  private AtomicBoolean serverDied = new AtomicBoolean(false);

  private CheckLivelinessThread checkLivelinessThread;

  private ServerAddressProvider addressProvider;

  private ServerClusterNodesProvider clusterNodesProvider;
  /** Address of our server. Only valid while started. */
  private ServerAddr ourAddr;

  public ServerControl(File serverJarFile, File workDir, ServerAddressProvider addressProvider,
      ServerClusterNodesProvider clusterNodesProvider) {
    this.serverJarFile = serverJarFile;
    this.workDir = workDir;
    this.addressProvider = addressProvider;
    this.clusterNodesProvider = clusterNodesProvider;
    serverLog = new File(workDir, "server.log");
    logbackConfig = createLogbackConfig(serverLog);
  }

  public void start() {
    startWithoutWait();
  }

  /* package */ void startWithoutWait() {
    if (isStarted())
      throw new RuntimeException("Server is started already.");

    ourAddr = addressProvider.reserveAddress();

    Properties serverProp = new Properties();
    serverProp.setProperty(ConfigKey.OUR_HOST, ourAddr.getHost());
    serverProp.setProperty(ConfigKey.PORT, Short.toString(ourAddr.getPort()));
    serverProp.setProperty(ConfigKey.DATA_DIR, workDir.getAbsolutePath());
    serverProp.setProperty(ConfigKey.CLUSTER_NODES, clusterNodesProvider.getClusterNodeConfigurationString(ourAddr));
    // TODO #38: support adjustment of the server properties by the tests

    serverPropertiesFile = new File(workDir, "server.properties");
    try (FileOutputStream propWrite = new FileOutputStream(serverPropertiesFile)) {
      serverProp.store(propWrite, "");
    } catch (IOException e1) {
      throw new RuntimeException("Could not write " + serverPropertiesFile.getAbsolutePath());
    }

    ProcessBuilder processBuilder = new ProcessBuilder("java",
        "-D" + ConfigurationManager.CUSTOM_PROPERTIES_SYSTEM_PROPERTY + "=" + serverPropertiesFile.getAbsolutePath(),
        "-Dlogback.configurationFile=" + logbackConfig.getAbsolutePath(), "-jar", serverJarFile.getAbsolutePath());

    serverDied.set(false);

    logger.info("Starting server at {}", ourAddr);

    try {
      serverProcess = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException("Could not start server.", e);
    }

    checkLivelinessThread = new CheckLivelinessThread(serverProcess, ourAddr);
    checkLivelinessThread.start();
  }

  /* package */ void waitUntilStarted() {
    waitUntilServerIsRunning(ourAddr);
  }

  public void stop() {
    if (serverDied.get()) {
      serverDied.set(false);
      throw new RuntimeException("The server " + ourAddr + " died unexpectedly before.");
    }

    if (!isStarted()) {
      logger.info("Server {} is stopped already.", ourAddr);
      return;
    }

    checkLivelinessThread.interrupt();
    try {
      checkLivelinessThread.join();
    } catch (InterruptedException e1) {
      // swallow.
    }
    serverDied.set(false);

    logger.info("Stopping server {}", ourAddr);

    try {
      serverProcess.destroy();

      boolean stopped = serverProcess.waitFor(10, TimeUnit.SECONDS);

      if (!stopped) {
        logger.error("The server {} did not stop, killing it forcibly.", ourAddr);
        serverProcess.destroyForcibly();
        throw new RuntimeException("The server " + ourAddr + " did not stop, killed it forcibly.");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting the server " + ourAddr + " to stop.", e);
    }
  }

  public boolean isStarted() {
    return serverProcess != null && serverProcess.isAlive();
  }

  public void deploy(File controlFile, File dataFile) {
    // TODO #53
  }

  public void undeploy(File controlFile) {
    // TODO #53
  }

  @Override
  public void saveLogfiles(File targetDir) {
    for (File logFile : Arrays.asList(serverLog, serverPropertiesFile)) {
      if (logFile.exists())
        try {
          Files.copy(logFile.toPath(), targetDir.toPath().resolve(logFile.getName()));
        } catch (IOException e) {
          logger.warn("Could not copy logfile {} to {}", logFile.getAbsolutePath(),
              targetDir.toPath().resolve(logFile.getName()), e);
        }
    }
  }

  private void waitUntilServerIsRunning(ServerAddr addr) {
    int maxWait = 100; // 10s
    Object sync = new Object();
    for (int i = 0; i < maxWait; i++) {
      synchronized (sync) {
        try {
          sync.wait(100);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for server to start up", e);
        }
      }

      try (TestConnection<KeepAliveService.Client> con = TestThriftConnectionFactory.open(addr.getHost(),
          addr.getPort(), KeepAliveService.Client.class, KeepAliveServiceConstants.SERVICE_NAME)) {

        con.getService().ping();

        // ping succeeded, server started up!
        logger.info("Server at {} has started successfully.", addr);
        return;
      } catch (IOException | TestConnectionException | TException e) {
        // swallow, could not open connection.
      }
    }

    logger.error("Server at {} did not startup within the timeout period!", addr);
    throw new RuntimeException("Server " + ourAddr + " did not start up.");
  }

  /**
   * Create a file containing a logback configuration which logs to the given logfile. Additionally it will log to
   * STDOUT, but only the messages of the logs (no loglevel, time etc.).
   */
  private File createLogbackConfig(File serverLog) {
    InputStream is = this.getClass().getResourceAsStream(LOGBACK_DEFAULT_CONFIG_CLASSPATH);
    File logbackOut = new File(workDir, "logback.xml");

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      ByteStreams.copy(is, baos);

      String logbackConfig = new String(baos.toByteArray(), Charset.forName("UTF-8"));
      logbackConfig = logbackConfig.replace("{{logfile}}", serverLog.getAbsolutePath().replace("\\", "\\\\"));
      byte[] replacedLogbackConfig = logbackConfig.getBytes("UTF-8");

      try (FileOutputStream fos = new FileOutputStream(logbackOut)) {
        fos.write(replacedLogbackConfig);
      }
    } catch (IOException e) {
      throw new RuntimeException("Could not create logback config", e);
    }

    return logbackOut;
  }

  /**
   * A {@link Thread} that continuously checks if the process of the server is still alive.
   */
  private class CheckLivelinessThread extends Thread {

    private Process serverProcess;
    private ServerAddr ourAddr;

    public CheckLivelinessThread(Process serverProcess, ServerAddr ourAddr) {
      super("server-liveliness-" + ourAddr.getHost() + "-" + ourAddr.getPort());
      this.serverProcess = serverProcess;
      this.ourAddr = ourAddr;
    }

    @Override
    public void run() {
      Object sync = new Object();
      while (true) {
        synchronized (sync) {
          try {
            sync.wait(1000);
          } catch (InterruptedException e) {
            // quiet exit.
            return;
          }
        }

        if (!serverProcess.isAlive()) {
          logger.error("Server {} died unexpectedly.", ourAddr);
          serverDied.set(true);
          return;
        }
      }
    }
  }

  /**
   * Represents the address of a server consisting of a host and a port.
   */
  public static class ServerAddr extends Pair<String, Short> {
    public ServerAddr(String left, Short right) {
      super(left, right);
    }

    public String getHost() {
      return getLeft();
    }

    public short getPort() {
      return getRight();
    }

    @Override
    public String toString() {
      return getHost() + ":" + getPort();
    }
  }

  /**
   * Provides a new server address to a starting server. The port has to be unbound currently.
   */
  public static interface ServerAddressProvider {
    public ServerAddr reserveAddress();
  }

  /**
   * Provides the addresses of other cluster nodes a new server should bind to in form of the configuration string for
   * {@link ConfigKey#CLUSTER_NODES}.
   */
  public static interface ServerClusterNodesProvider {
    /**
     * Calculates and returns the string to be used for {@link ConfigKey#CLUSTER_NODES} to be used for the given cluster
     * node.
     */
    public String getClusterNodeConfigurationString(ServerAddr serverAddr);
  }

}
