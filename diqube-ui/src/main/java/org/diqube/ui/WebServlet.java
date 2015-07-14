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

import java.io.IOException;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RNodeHttpAddress;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.QueryService;

/**
 *
 * @author Bastian Gloeckle
 */
public class WebServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    TTransport transport = new TSocket("localhost", 5101);
    transport = new TFramedTransport(transport);
    TProtocol queryProtocol =
        new TMultiplexedProtocol(new TCompactProtocol(transport), QueryServiceConstants.SERVICE_NAME);

    QueryService.Client queryClient = new QueryService.Client(queryProtocol);

    try {
      transport.open();

      UUID queryUuid = UUID.randomUUID();

      queryClient.asyncExecuteQuery(RUuidUtil.toRUuid(queryUuid), //
          "select age, count() from age group by age order by count() desc", //
          true, createOurAddress(req));

      resp.getWriter().write("Sent out query " + queryUuid.toString());
    } catch (TException e) {
      resp.getWriter().write("Exception: " + e.getMessage());
    }
    resp.getWriter().write("\n\n\ndiqube is distributed under the terms of the GNU Affero General Public License 3, "
        + "its source code is available at https://github.com/diqube.");
  }

  private RNodeAddress createOurAddress(HttpServletRequest req) {
    String responseUrl = // TODO make configurable
        req.getScheme() + "://" + req.getServerName() + ":" + req.getServerPort()
            + req.getServletContext().getContextPath() + ThriftServlet.URL_PATTERN;

    RNodeAddress res = new RNodeAddress();
    res.setHttpAddr(new RNodeHttpAddress());
    res.getHttpAddr().setUrl(responseUrl);
    return res;
  }
}
