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
package org.diqube.remote.query;

import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.diqube.remote.base.services.DiqubeThriftService;
import org.diqube.remote.query.thrift.ClusterInformationService;

/**
 * Constants to be used when using {@link ClusterInformationService}.
 *
 * @author Bastian Gloeckle
 */
@DiqubeThriftService( //
    serviceInterface = ClusterInformationService.Iface.class, //
    clientClass = ClusterInformationService.Client.class, //
    serviceName = ClusterInformationServiceConstants.SERVICE_NAME, //
    integrityChecked = false)
public class ClusterInformationServiceConstants {
  /** Name of the query service as set up in {@link TMultiplexedProtocol}. */
  public static final String SERVICE_NAME = "I";
}
