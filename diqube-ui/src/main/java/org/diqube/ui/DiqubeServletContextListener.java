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

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.websocket.DeploymentException;
import javax.websocket.server.ServerContainer;
import javax.websocket.server.ServerEndpointConfig;

import org.diqube.ui.websocket.WebSocketEndpoint;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Simple {@link ServletContextListener} which initializes {@link DiqubeServletConfig}.
 * 
 * Needs to be executed after the Spring context has been instantiated already (the {@link ContextLoaderListener} ).
 *
 * @author Bastian Gloeckle
 */
public class DiqubeServletContextListener implements ServletContextListener {

  /**
   * Attribute name of a {@link ServletContext} attribute containing the {@link ServerContainer} as specified in JSR 356
   * Websocket API 1.1, 6.4 "Programmatic Server Deployment"
   */
  private static final String ATTR_SERVER_CONTAINER = "javax.websocket.server.ServerContainer";

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    // initialize DiqubeServletConfig
    WebApplicationContext ctx = WebApplicationContextUtils.getRequiredWebApplicationContext(sce.getServletContext());
    ctx.getBean(DiqubeServletConfig.class).initialize(sce.getServletContext());

    // register our Websocket Endpoint
    ServerContainer serverContainer = (ServerContainer) sce.getServletContext().getAttribute(ATTR_SERVER_CONTAINER);

    ServerEndpointConfig sec =
        ServerEndpointConfig.Builder.create(WebSocketEndpoint.class, WebSocketEndpoint.ENDPOINT_URL_MAPPING).build();
    sec.getUserProperties().put(WebSocketEndpoint.PROP_BEAN_CONTEXT, ctx);

    try {
      serverContainer.addEndpoint(sec);
    } catch (DeploymentException e) {
      throw new RuntimeException("DeploymentException when deploying Websocket endpoint", e);
    }
  }

}
