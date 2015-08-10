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
package org.diqube.ui.websocket.json;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RNodeHttpAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;
import org.diqube.ui.DiqubeServletContextListener;
import org.diqube.ui.QueryResultRegistry;
import org.diqube.ui.ThriftServlet;
import org.diqube.util.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Bastian Gloeckle
 */
public class JsonQueryCommand extends JsonCommand {

  @JsonProperty
  public String diql;

  @Override
  public void execute() throws RuntimeException {
    Set<Integer> idxToCheck =
        IntStream.range(0, DiqubeServletContextListener.clusterServers.size()).boxed().collect(Collectors.toSet());

    UUID queryUuid = null;
    while (queryUuid == null) {
      if (idxToCheck.isEmpty())
        throw new RuntimeException("No cluster servers were reachable");
      int nextIdx = (int) Math.floor(Math.random() * DiqubeServletContextListener.clusterServers.size());
      if (!idxToCheck.remove(nextIdx))
        continue;

      queryUuid =
          sendDiqlQuery(DiqubeServletContextListener.clusterServers.get(nextIdx), new QueryResultService.Iface() {
            @Override
            public void queryStatistics(RUUID queryRuuid, RQueryStatistics stats) throws TException {
            }

            @Override
            public void queryResults(RUUID queryRUuid, RResultTable finalResult) throws TException {
              sendResult(RUuidUtil.toUuid(queryRUuid), finalResult, true);
            }

            private void sendResult(UUID queryUuid, RResultTable finalResult, boolean doUnregister) {
              JsonQueryResultCommand resCommand = new JsonQueryResultCommand();
              resCommand.setColumnNames(finalResult.getColumnNames());
              List<List<Object>> rows = new ArrayList<>();
              for (List<RValue> incomingResultRow : finalResult.getRows()) {
                List<Object> row = incomingResultRow.stream().map(rValue -> RValueUtil.createValue(rValue))
                    .collect(Collectors.toList());
                rows.add(row);
              }
              resCommand.setRows(rows);

              ObjectMapper mapper = new ObjectMapper();
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              OutputStreamWriter osw = new OutputStreamWriter(baos, Charset.forName("UTF-8"));
              try {
                mapper.writerFor(JsonQueryResultCommand.class).writeValue(osw, resCommand);
                osw.close();

                getWebsocketSession().getAsyncRemote()
                    .sendText(new String(baos.toByteArray(), Charset.forName("UTF-8")));

                if (doUnregister) {
                  QueryResultRegistry.unregister(queryUuid);
                  getWebsocketSession().close();
                }
              } catch (IllegalStateException e) {
                // Session seems to be closed.
                System.out.println("Session seems to be closed.");
              } catch (IOException e) {
                e.printStackTrace();
              }
            }

            @Override
            public void queryException(RUUID queryRUuid, RQueryException exceptionThrown) throws TException {
              QueryResultRegistry.unregister(RUuidUtil.toUuid(queryRUuid));
              try {
                getWebsocketSession().close();
              } catch (IOException e) {
                // TODO
              }
            }

            @Override
            public void partialUpdate(RUUID queryRUuid, RResultTable partialResult, short percentComplete)
                throws TException {
              sendResult(RUuidUtil.toUuid(queryRUuid), partialResult, false);
            }
          });
    }
  }

  private UUID sendDiqlQuery(Pair<String, Short> node, QueryResultService.Iface resultHandler) {
    TTransport transport = new TSocket(node.getLeft(), node.getRight());
    transport = new TFramedTransport(transport);
    TProtocol queryProtocol =
        new TMultiplexedProtocol(new TCompactProtocol(transport), QueryServiceConstants.SERVICE_NAME);

    QueryService.Client queryClient = new QueryService.Client(queryProtocol);

    UUID queryUuid = UUID.randomUUID();
    try {
      transport.open();

      QueryResultRegistry.registerThriftResultCallback(queryUuid, resultHandler);

      queryClient.asyncExecuteQuery(RUuidUtil.toRUuid(queryUuid), //
          diql, //
          true, createOurAddress());
      return queryUuid;
    } catch (RQueryException e) {
      QueryResultRegistry.unregister(queryUuid);
      throw new RuntimeException("Query error: " + e.getMessage());
    } catch (TException e) {
      QueryResultRegistry.unregister(queryUuid);
      return null;
    }
  }

  private RNodeAddress createOurAddress() {
    RNodeAddress res = new RNodeAddress();
    res.setHttpAddr(new RNodeHttpAddress());
    res.getHttpAddr().setUrl(DiqubeServletContextListener.clusterResponseAddr + ThriftServlet.URL_PATTERN);
    return res;
  }

}
