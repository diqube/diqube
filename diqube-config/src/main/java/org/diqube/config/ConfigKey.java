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
package org.diqube.config;

/**
 * Configuration keys which can be used to resolve configuration values.
 * 
 * <p>
 * It's easiest to use these constants with the {@link Config} annotation.
 *
 * @author Bastian Gloeckle
 */
public class ConfigKey {
  /**
   * The main TCP port the server should use. The Thrift server will bind to this.
   */
  public static final String PORT = "port";

  /**
   * The number of selector threads used by the Thrift server.
   * 
   * <p>
   * The Thrift server has a single accept thread which will accept new connections. It then hands over those new
   * connections to a thread pool of selector threads which will read and write the connection. The actual processing of
   * the request will not happen within these selector threads, as each connection could be used to issue multiple
   * computations simultaneously. The number of selectorThreads will limit the number of concurrent connections the
   * server can read from and write to simultaneously.
   */
  public static final String SELECTOR_THREADS = "selectorThreads";

  /**
   * The directory which should be watched for new data to be loaded. See NewDataWatcher for more details.
   * 
   * <p>
   * This can be a relative path which is then interpreted as being relative to the current working directory.
   */
  public static final String DATA_DIR = "dataDir";

  /**
   * The host of this node which is usable by other cluster nodes to communicate with this node.
   * 
   * This can either be an IP address or a hostname that is resolved via DNS.
   * 
   * Special value "*" is for automatic detection. Have a look at the log messages after startup to find out which value
   * was chosen.
   */
  public static final String OUR_HOST = "host";

  /**
   * The IP to bind the server socket to.
   * 
   * Leave empty to bind to all interfaces.
   */
  public static final String BIND = "bind";

  /**
   * A few addresses of nodes in the diqube cluster this node should connect to.
   * 
   * Format is: <code>
   * host:port,host:post,host:port, ...
   * </code>
   */
  public static final String CLUSTER_NODES = "clusterNodes";

  /**
   * Timeout that should be used for connections this node opens to other nodes. Value is in milliseconds.
   * 
   * This timeout is for opening connections and reading data from the remote.
   */
  public static final String CLIENT_SOCKET_TIMEOUT_MS = "clientSocketTimeoutMs";

  /**
   * Time in milliseconds between keep alives that are sent to all nodes.
   */
  public static final String KEEP_ALIVE_MS = "keepAliveMs";

  /**
   * Limit of how many connections should be opened overall from this node.
   * 
   * This is a "soft" limit, as in some circumstances diqube might open slightly more connections than this limit, which
   * is e.g. to ensure that no deadlocks occur.
   */
  public static final String CONNECTION_SOFT_LIMIT = "connectionSoftLimit";

  /**
   * The milliseconds a connection may be idle before it is allowed to be closed. Please note that it might take more
   * time until the connection is actually closed.
   */
  public static final String CONNECTION_IDLE_TIME_MS = "connectionIdleTimeMs";

  /**
   * A percentage (= Double value) of the currently opened connections compared to the {@link #CONNECTION_SOFT_LIMIT}.
   * 
   * As soon as the given level of open connections is reached, diqube tries to close currently opened but unused
   * connections before these reach {@link #CONNECTION_IDLE_TIME_MS}.
   */
  public static final String CONNECTION_EARLY_CLOSE_LEVEL = "connectionEarlyCloseLevel";
}
