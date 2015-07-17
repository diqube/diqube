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

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.QueryResultServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryResultService;

/**
 *
 * @author Bastian Gloeckle
 */
public class ThriftServlet extends TServlet {

  /** make sure this matches the web.xml. */
  public static final String URL_PATTERN = "/t";

  private static final long serialVersionUID = 1L;

  public ThriftServlet() {
    super(createProcessor(), createProtocolFactory());
  }

  private static TProcessor createProcessor() {
    TMultiplexedProcessor res = new TMultiplexedProcessor();

    res.registerProcessor(QueryResultServiceConstants.SERVICE_NAME,
        new QueryResultService.Processor<QueryResultService.Iface>(new QueryResultServiceHandler()));
    res.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME,
        new KeepAliveService.Processor<KeepAliveService.Iface>(new KeepAliveServiceHandler()));

    return res;
  }

  private static TProtocolFactory createProtocolFactory() {
    return new TCompactProtocol.Factory();
  }
}
