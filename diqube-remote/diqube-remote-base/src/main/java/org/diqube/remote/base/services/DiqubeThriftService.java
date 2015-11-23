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
package org.diqube.remote.base.services;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TMultiplexedProtocol;

/**
 * Specify details of a remote-callable thrift service of diqube.
 *
 * @author Bastian Gloeckle
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DiqubeThriftService {
  /**
   * @return The (Thrift-generated) Interface that is implemented by service handlers.
   */
  Class<?> serviceInterface();

  /**
   * @return Thrift "Client" class.
   */
  Class<? extends TServiceClient> clientClass();

  /**
   * @return Name of the service for the {@link TMultiplexedProtocol}.
   */
  String serviceName();

  /**
   * @return true if access to the service needs to contain integrity check information, false if not.
   */
  boolean integrityChecked();
}
