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

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.diqube.util.Pair;

/**
 *
 * @author Bastian Gloeckle
 */
@WebListener
public class DiqubeServletContextListener implements ServletContextListener {

  public static final String INIT_PARAM_CLUSTER = "diqube.cluster";
  public static final String INIT_PARAM_CLUSTER_RESPONSE = "diqube.clusterresponse";
  public static final String DEFAULT_CLUSTER = "localhost:5101";
  public static final String DEFAULT_CLUSTER_RESPONSE = "http://localhost:8080";

  // TODO remove static access, but provide a real context.
  public static List<Pair<String, Short>> clusterServers;

  public static String clusterResponseAddr;

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    String clusterLocation = sce.getServletContext().getInitParameter(INIT_PARAM_CLUSTER);
    if (clusterLocation == null)
      clusterLocation = DEFAULT_CLUSTER;

    clusterServers = new ArrayList<>();
    for (String hostPortStr : clusterLocation.split(",")) {
      String[] split = hostPortStr.split(":");
      clusterServers.add(new Pair<>(split[0], Short.valueOf(split[1])));
    }
    clusterServers = Collections.unmodifiableList(clusterServers);

    String clusterResponse = sce.getServletContext().getInitParameter(INIT_PARAM_CLUSTER_RESPONSE);
    if (clusterResponse == null)
      clusterResponse = DEFAULT_CLUSTER_RESPONSE;

    clusterResponseAddr = clusterResponse + sce.getServletContext().getContextPath();
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
  }

}
