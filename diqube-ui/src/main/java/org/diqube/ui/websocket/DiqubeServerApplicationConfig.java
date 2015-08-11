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
package org.diqube.ui.websocket;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.websocket.Endpoint;
import javax.websocket.server.ServerApplicationConfig;
import javax.websocket.server.ServerEndpointConfig;

/**
 * A {@link ServerApplicationConfig} that returns empty results, which leads to disabling the instantiation of
 * class-path scanned websocket endpoints (as of JSR 356, v1.1, 6.4 "Programmatic Server Deployment").
 * 
 * We need to deploy our endpoint manually, because our endpoint needs to have the {@link ServletContext} available,
 * which we cannot get in the automatic deployment model.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeServerApplicationConfig implements ServerApplicationConfig {

  @Override
  public Set<ServerEndpointConfig> getEndpointConfigs(Set<Class<? extends Endpoint>> endpointClasses) {
    return new HashSet<>();
  }

  @Override
  public Set<Class<?>> getAnnotatedEndpointClasses(Set<Class<?>> scanned) {
    return new HashSet<>();
  }

}
