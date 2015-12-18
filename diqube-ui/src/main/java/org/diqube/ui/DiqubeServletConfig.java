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
package org.diqube.ui;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContext;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.RNodeHttpAddress;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.db.UiDatabase;
import org.diqube.util.Pair;

import com.google.common.collect.Sets;

/**
 * Configuration for the diqube UI.
 * 
 * Loads the config from servlet context init params or chooses defaults if it finds none. The init params can be
 * overwritten in the container somewhere, see container specific documentation.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeServletConfig {

  // For a description of what these configuration keys mean, see the #get.. methods below.
  public static final String INIT_PARAM_CLUSTER = "diqube.cluster";
  public static final String INIT_PARAM_CLUSTER_RESPONSE = "diqube.clusterresponse";
  public static final String INIT_PARAM_TICKET_PUBLIC_KEY_PEM = "diqube.ticketPublicKeyPem";
  public static final String INIT_PARAM_TICKET_PUBLIC_KEY_PEM_ALT1 = "diqube.ticketPublicKeyPemAlt1";
  public static final String INIT_PARAM_TICKET_PUBLIC_KEY_PEM_ALT2 = "diqube.ticketPublicKeyPemAlt2";
  public static final String INIT_PARAM_LOGOUT_TICKET_FETCH_SEC = "diqube.logoutTicketFetchSec";
  public static final String INIT_PARAM_UI_DB_TYPE = "diqube.uiDbType";
  public static final String INIT_PARAM_UI_DB_LOCATION = "diqube.uiDbLocation";
  public static final String INIT_PARAM_UI_DB_USER = "diqube.uiDbUser";
  public static final String INIT_PARAM_UI_DB_PASSWORD = "diqube.uiDbPassword";

  public static final String UI_DB_TYPE_HSQLDB = "hsqldb";

  public static final String DEFAULT_CLUSTER = "localhost:5101";
  public static final String DEFAULT_CLUSTER_RESPONSE = "http://localhost:8080";
  public static final String DEFAULT_LOGOUT_TICKET_FETCH_SEC = "120";
  public static final String DEFAULT_UI_DB_TYPE = UI_DB_TYPE_HSQLDB;
  public static final String DEFAULT_UI_DB_LOCATION = "diqube-ui.db";

  @InjectOptional
  private List<ServletConfigListener> servletConfigListeners;

  private List<Pair<String, Short>> clusterServers;

  private String clusterResponseAddr;
  private String ticketPublicKeyPem;
  private String ticketPublicKeyPemAlt1;
  private String ticketPublicKeyPemAlt2;
  private long logoutTicketFetchSec;
  private String uiDbType;
  private String uiDbLocation;
  private String uiDbUser;
  private String uiDbPassword;

  /* package */ void initialize(ServletContext ctx) {
    String clusterLocation = ctx.getInitParameter(INIT_PARAM_CLUSTER);
    if (clusterLocation == null)
      clusterLocation = DEFAULT_CLUSTER;

    clusterServers = new ArrayList<>();
    for (String hostPortStr : clusterLocation.split(",")) {
      String[] split = hostPortStr.split(":");
      clusterServers.add(new Pair<>(split[0], Short.valueOf(split[1])));
    }
    clusterServers = Collections.unmodifiableList(clusterServers);

    String clusterResponse = ctx.getInitParameter(INIT_PARAM_CLUSTER_RESPONSE);
    if (clusterResponse == null)
      clusterResponse = DEFAULT_CLUSTER_RESPONSE;

    clusterResponseAddr = clusterResponse + ctx.getContextPath();

    ticketPublicKeyPem = ctx.getInitParameter(INIT_PARAM_TICKET_PUBLIC_KEY_PEM);
    if (ticketPublicKeyPem == null)
      throw new RuntimeException("Value for paramater '" + INIT_PARAM_TICKET_PUBLIC_KEY_PEM + "' needed.");
    ticketPublicKeyPemAlt1 = ctx.getInitParameter(INIT_PARAM_TICKET_PUBLIC_KEY_PEM_ALT1);
    ticketPublicKeyPemAlt2 = ctx.getInitParameter(INIT_PARAM_TICKET_PUBLIC_KEY_PEM_ALT2);

    String logoutTicketFetchSecString = ctx.getInitParameter(INIT_PARAM_LOGOUT_TICKET_FETCH_SEC);
    if (logoutTicketFetchSecString == null)
      logoutTicketFetchSecString = DEFAULT_LOGOUT_TICKET_FETCH_SEC;
    logoutTicketFetchSec = Long.parseLong(logoutTicketFetchSecString);

    uiDbType = ctx.getInitParameter(INIT_PARAM_UI_DB_TYPE);
    if (uiDbType == null)
      uiDbType = DEFAULT_UI_DB_TYPE;
    if (!Sets.newHashSet(UI_DB_TYPE_HSQLDB).contains(uiDbType))
      throw new RuntimeException("Unknown " + INIT_PARAM_UI_DB_TYPE + ": " + uiDbType);

    uiDbLocation = ctx.getInitParameter(INIT_PARAM_UI_DB_LOCATION);
    if (uiDbLocation == null)
      uiDbLocation = DEFAULT_UI_DB_LOCATION;

    uiDbUser = ctx.getInitParameter(INIT_PARAM_UI_DB_USER);
    uiDbPassword = ctx.getInitParameter(INIT_PARAM_UI_DB_PASSWORD);

    if (servletConfigListeners != null)
      servletConfigListeners.forEach(l -> l.servletConfigurationAvailable());
  }

  /**
   * @return List of Pair of hostname, port of the seed cluster nodes.
   */
  public List<Pair<String, Short>> getClusterServers() {
    return clusterServers;
  }

  /**
   * @return The address of this UI that cluster nodes can use to send responses to after we issued a query for example.
   *         Example value: http://localhost:8080/diqube-ui. It does not contain a trailing / and contains the whole URL
   *         up until the root of this web application (no servlet-specific part!).
   */
  public String getClusterResponseAddr() {
    return clusterResponseAddr;
  }

  /**
   * @return A new instance of {@link RNodeAddress} that contains {@link #getClusterResponseAddr()}.
   */
  public RNodeAddress createClusterResponseAddr() {
    RNodeAddress res = new RNodeAddress();
    res.setHttpAddr(new RNodeHttpAddress());
    res.getHttpAddr().setUrl(getClusterResponseAddr() + ThriftServlet.URL_PATTERN);
    return res;
  }

  /**
   * @return File name of the .pem file containing a RSA public key which can be used to validate {@link Ticket}s.
   */
  public String getTicketPublicKeyPem() {
    return ticketPublicKeyPem;
  }

  /**
   * @return File name of the .pem file containing a RSA public key which can be used to validate {@link Ticket}s. This
   *         can be <code>null</code>.
   */
  public String getTicketPublicKeyPemAlt1() {
    return ticketPublicKeyPemAlt1;
  }

  /**
   * @return File name of the .pem file containing a RSA public key which can be used to validate {@link Ticket}s. This
   *         can be <code>null</code>.
   */
  public String getTicketPublicKeyPemAlt2() {
    return ticketPublicKeyPemAlt2;
  }

  /**
   * @return Duration in seconds after which a current list of logged out {@link Ticket}s should be fetched from a
   *         diqube-server. If logged out tickets cannot be fetched, the UI will not accept any {@link Ticket} anymore
   *         until connection is re-established! Note that the longer this time is, the longer the UI might accept
   *         tickets which actually have logged out already!
   */
  public long getLogoutTicketFetchSec() {
    return logoutTicketFetchSec;
  }

  /**
   * @return Type of the database the UI should use.
   */
  public String getUiDbType() {
    return uiDbType;
  }

  /**
   * @return DB-type dependent information on where the db is. Used in connection string. For more details, see JavaDoc
   *         of implementation class of {@link UiDatabase}.
   */
  public String getUiDbLocation() {
    return uiDbLocation;
  }

  /**
   * @return Username to use to connect to UI DB. Optional, if needed by DB implementation.
   */
  public String getUiDbUser() {
    return uiDbUser;
  }

  /**
   * @return Password to use to connect to UI DB. Optional, if needed by DB implementation.
   */
  public String getUiDbPassword() {
    return uiDbPassword;
  }

  /**
   * Simple listener interface that gets called as soon as the {@link DiqubeServletConfig} is initialized.
   */
  public static interface ServletConfigListener {
    public void servletConfigurationAvailable();
  }
}
