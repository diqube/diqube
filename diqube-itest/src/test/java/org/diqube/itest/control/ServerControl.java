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
package org.diqube.itest.control;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.diqube.config.ConfigKey;
import org.diqube.config.ConfigurationManager;
import org.diqube.connection.ConnectionFactory;
import org.diqube.connection.DefaultDiqubeConnectionFactoryTestUtil;
import org.diqube.itest.annotations.NeedsProcessPid;
import org.diqube.itest.util.ProcessPidUtil;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.Waiter;
import org.diqube.itest.util.Waiter.WaitTimeoutException;
import org.diqube.remote.base.services.DiqubeThriftServiceInfoManager;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RNodeDefaultAddress;
import org.diqube.server.ControlFileLoader;
import org.diqube.server.NewDataWatcher;
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

  /** Control over this server was manually overridden, this class cannot start/stop this server! */
  private boolean manualOverride;

  private ProcessOutputReadThread processOutputReadThread;

  private ServiceTestUtil serviceTestUtil;

  /** key for HMAC the server uses for thrift access */
  private String serverMacKey;

  public ServerControl(File serverJarFile, File workDir, ServerAddressProvider addressProvider,
      ServerClusterNodesProvider clusterNodesProvider, boolean manualOverride) {
    this.serverJarFile = serverJarFile;
    this.workDir = workDir;
    this.addressProvider = addressProvider;
    this.clusterNodesProvider = clusterNodesProvider;
    this.manualOverride = manualOverride;
    serverLog = new File(workDir, "server.log");
    logbackConfig = createLogbackConfig(serverLog);
  }

  public void start() {
    start(null);
  }

  public void start(Consumer<Properties> serverPropertiesAdjust) {
    if (!manualOverride && isStarted())
      throw new RuntimeException("Server is started already.");

    ourAddr = addressProvider.reserveAddress();

    serverMacKey = "thisisatest";

    if (manualOverride) {
      logger.info("Using already started server at port {}, not starting a separate one!", ourAddr.getPort());
      serverDied.set(false);
      serverProcess = null;
      checkLivelinessThread = null;
    } else {
      Properties serverProp = new Properties();
      serverProp.setProperty(ConfigKey.OUR_HOST, ourAddr.getHost());
      serverProp.setProperty(ConfigKey.PORT, Short.toString(ourAddr.getPort()));
      serverProp.setProperty(ConfigKey.DATA_DIR, workDir.getAbsolutePath());
      serverProp.setProperty(ConfigKey.CLUSTER_NODES, clusterNodesProvider.getClusterNodeConfigurationString(ourAddr));
      serverProp.setProperty(ConfigKey.BIND, ourAddr.getHost()); // bind only to our addr, which is typically 127.0.0.1
      serverProp.setProperty(ConfigKey.MESSAGE_INTEGRITY_SECRET, serverMacKey);
      if (serverPropertiesAdjust != null)
        serverPropertiesAdjust.accept(serverProp);

      serverPropertiesFile = new File(workDir, "server.properties");
      try (FileOutputStream propWrite = new FileOutputStream(serverPropertiesFile)) {
        serverProp.store(propWrite, "");
      } catch (IOException e1) {
        throw new RuntimeException("Could not write " + serverPropertiesFile.getAbsolutePath());
      }

      ProcessBuilder processBuilder = new ProcessBuilder("java",
          "-D" + ConfigurationManager.CUSTOM_PROPERTIES_SYSTEM_PROPERTY + "=" + serverPropertiesFile.getAbsolutePath(),
          "-Dlogback.configurationFile=" + logbackConfig.getAbsolutePath(), "-jar", serverJarFile.getAbsolutePath());

      processBuilder.redirectErrorStream(true);

      serverDied.set(false);

      logger.info("Starting server at {}", ourAddr);

      try {
        serverProcess = processBuilder.start();
      } catch (IOException e) {
        throw new RuntimeException("Could not start server.", e);
      }

      checkLivelinessThread = new CheckLivelinessThread(serverProcess, ourAddr);
      checkLivelinessThread.start();

      processOutputReadThread = new ProcessOutputReadThread(serverProcess.getInputStream(), ourAddr);
      processOutputReadThread.start();

      try {
        // seems to be a good idea to close the input of the process right away as we'll never send any data to it.
        serverProcess.getOutputStream().close();
      } catch (IOException e) {
        logger.debug("Could not close input pipe of server process", e);
      }

      waitUntilServerIsRunning(ourAddr);
    }
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

    if (!manualOverride) {
      checkLivelinessThread.interrupt();
      processOutputReadThread.interrupt();
      try {
        checkLivelinessThread.join();
        processOutputReadThread.join();
      } catch (InterruptedException e1) {
        // swallow.
      }
    }
    serverDied.set(false);

    if (!manualOverride) {
      logger.info("Stopping server {}", ourAddr);

      try {
        serverProcess.destroy();

        boolean stopped = serverProcess.waitFor(10, TimeUnit.SECONDS);

        // cleanup ready files
        for (File readyFile : workDir.listFiles((dir, file) -> file.endsWith(NewDataWatcher.READY_FILE_EXTENSION)))
          if (!readyFile.delete())
            logger.warn("Could not delete ready file {}", readyFile);

        if (!stopped) {
          logger.error("The server {} did not stop, killing it forcibly.", ourAddr);
          serverProcess.destroyForcibly();
          throw new RuntimeException("The server " + ourAddr + " did not stop, killed it forcibly.");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting the server " + ourAddr + " to stop.", e);
      }
    } else
      logger.warn("Cannot stop server at {} as it was manually overridden!", ourAddr);
  }

  /**
   * Create a thread dump of the running server and output it to the given file.
   * 
   * <p>
   * This depends on
   * <ul>
   * <li>{@link ProcessPidUtil} to work. Use {@link NeedsProcessPid} annotation on test method.
   * <li>"jstack" to be installed on the host machine
   * </ul>
   */
  public void createThreadDump(OutputStream outstream) {
    int serverPid = ProcessPidUtil.getPid(serverProcess);
    ProcessBuilder jstackProcessBuilder = new ProcessBuilder("jstack", Integer.toString(serverPid));

    try {
      Process jstackProcess = jstackProcessBuilder.start();
      for (int i = 0; i < 10; i++) {
        jstackProcess.waitFor(1, TimeUnit.SECONDS);
        ByteStreams.copy(jstackProcess.getInputStream(), outstream);
      }

      ByteStreams.copy(jstackProcess.getInputStream(), outstream);
      if (!jstackProcess.waitFor(1, TimeUnit.SECONDS)) {
        jstackProcess.destroyForcibly();
        throw new RuntimeException("jstack process did not shut down, killed it forcibly.");
      }
      ByteStreams.copy(jstackProcess.getInputStream(), outstream);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    } catch (IOException e) {
      throw new RuntimeException("IOException while interacting with jstack.", e);
    }
  }

  /**
   * All the output of the server process.
   */
  public String getServerLogOutput() {
    return new String(processOutputReadThread.getCurrentBytes(), Charset.forName("UTF-8"));
  }

  public boolean isStarted() {
    return manualOverride || serverProcess != null && serverProcess.isAlive();
  }

  public ServiceTestUtil getSerivceTestUtil() {
    if (serviceTestUtil == null) {
      DiqubeThriftServiceInfoManager infoManager = new DiqubeThriftServiceInfoManager();
      infoManager.initialize();

      ConnectionFactory connectionFactory =
          DefaultDiqubeConnectionFactoryTestUtil.createDefaultConnectionFactory(serverMacKey);

      serviceTestUtil = new ServiceTestUtil(this, connectionFactory, infoManager);
    }
    return serviceTestUtil;
  }

  private File readyFile(File controlFile) {
    return new File(controlFile.getParentFile(),
        controlFile.getName().substring(0,
            controlFile.getName().length() - NewDataWatcher.CONTROL_FILE_EXTENSION.length())
        + NewDataWatcher.READY_FILE_EXTENSION);
  }

  public void deploy(File controlFile, File dataFile) throws WaitTimeoutException {
    Properties control = new Properties();
    try (FileInputStream is = new FileInputStream(controlFile)) {
      control.load(new InputStreamReader(is, Charset.forName("UTF-8")));
    } catch (IOException e) {
      throw new RuntimeException("Could not load control file properties from " + controlFile.getAbsolutePath());
    }

    control.setProperty(ControlFileLoader.KEY_FILE, dataFile.getAbsolutePath());

    // put the adjusted .control file into workDir, as our server is watching that directory!
    File realControlFile = new File(workDir, controlFile.getName());

    try (FileOutputStream fos = new FileOutputStream(realControlFile)) {
      control.store(new OutputStreamWriter(fos, Charset.forName("UTF-8")), "");
    } catch (IOException e) {
      throw new RuntimeException("Could not store control file to " + realControlFile.getAbsolutePath());
    }

    // wait for ready file to get present, if started
    if (isStarted())
      waitUntilDeployed(controlFile);
  }

  public void waitUntilDeployed(File controlFile) {
    File realControlFile = new File(workDir, controlFile.getName());
    File readyFile = readyFile(realControlFile);
    new Waiter().waitUntil("Ready file " + readyFile.getAbsolutePath() + " to be available", 120, 500,
        () -> readyFile.exists());
  }

  public void undeploy(File controlFile) {
    File realControlFile = new File(workDir, controlFile.getName());

    if (!realControlFile.delete())
      throw new RuntimeException("Could not delete control file " + realControlFile.getAbsolutePath());

    // wait until ready file is deleted, too, if started
    if (isStarted())
      waitUntilUndeployed(controlFile);
  }

  public void waitUntilUndeployed(File controlFile) {
    File realControlFile = new File(workDir, controlFile.getName());
    File readyFile = readyFile(realControlFile);
    new Waiter().waitUntil("Ready file " + readyFile.getAbsolutePath() + " is deleted", 20, 100,
        () -> !readyFile.exists());
  }

  public ServerAddr getAddr() {
    return ourAddr;
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
    new Waiter().waitUntil("Server at " + addr + " to start up", 10, 100, () -> {
      try {
        getSerivceTestUtil().keepAliveService(service -> service.ping());
        // ping succeeded, server started up!
        logger.info("Server at {} has started successfully.", addr);
        return true;
      } catch (RuntimeException e) {
        return false;
      }
    });
  }

  public byte[] getServerMacKey() {
    return serverMacKey.getBytes(Charset.forName("UTF-8"));
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
   * Thread that continuously reads from an {@link InputStream} and provides all the read bytes.
   */
  private class ProcessOutputReadThread extends Thread {

    private InputStream streamToReadFrom;
    private ByteArrayOutputStream bytesReadStream = new ByteArrayOutputStream();

    public ProcessOutputReadThread(InputStream streamToReadFrom, ServerAddr server) {
      super("server-output-read-" + server.getHost() + "-" + server.getPort());
      this.streamToReadFrom = streamToReadFrom;
    }

    @Override
    public void run() {
      Object sync = new Object();
      while (true) {
        synchronized (sync) {
          try {
            sync.wait(10);
          } catch (InterruptedException e) {
            // quiet exit
            return;
          }
        }

        int read;
        try {
          if (streamToReadFrom.available() > 0) {
            byte[] buf = new byte[streamToReadFrom.available()];
            if ((read = streamToReadFrom.read(buf)) > 0) {
              bytesReadStream.write(buf, 0, read);
            }
          }
        } catch (IOException e) {
          logger.error("IOException while reading from server process", e);
          throw new RuntimeException("IOException while reading from server process", e);
        }
      }
    }

    public byte[] getCurrentBytes() {
      return bytesReadStream.toByteArray();
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

    public RNodeAddress toRNodeAddress() {
      RNodeAddress res = new RNodeAddress();
      res.setDefaultAddr(new RNodeDefaultAddress());
      res.getDefaultAddr().setHost(getHost());
      res.getDefaultAddr().setPort(getPort());
      return res;
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
