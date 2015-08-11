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

import java.io.IOException;
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
import org.diqube.ui.websocket.json.JsonPayloadSerializer.JsonPayloadSerializerException;
import org.diqube.util.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class JsonQueryCommand extends JsonCommand {

  public static final String PAYLOAD_TYPE = "query";

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
              sendResult(RUuidUtil.toUuid(queryRUuid), finalResult, 100, true);
            }

            private void sendResult(UUID queryUuid, RResultTable finalResult, int percentComplete,
                boolean doUnregister) {
              JsonQueryResultPayload res = new JsonQueryResultPayload();
              res.setColumnNames(finalResult.getColumnNames());
              List<List<Object>> rows = new ArrayList<>();
              for (List<RValue> incomingResultRow : finalResult.getRows()) {
                List<Object> row = incomingResultRow.stream().map(rValue -> RValueUtil.createValue(rValue))
                    .collect(Collectors.toList());
                rows.add(row);
              }
              res.setRows(rows);
              res.setPercentComplete((short) percentComplete);

              try {
                String resString = new JsonPayloadSerializer().serialize(res);

                getWebsocketSession().getAsyncRemote().sendText(resString);

                if (doUnregister) {
                  QueryResultRegistry.unregister(queryUuid);
                  getWebsocketSession().close();
                }
              } catch (IllegalStateException e) {
                // Session seems to be closed.
                System.out.println("Session seems to be closed.");
              } catch (IOException e) {
                System.out.println("Could not close session: " + e);
              } catch (JsonPayloadSerializerException e) {
                System.err.println("Could not serialize result: " + e);
              }
            }

            @Override
            public void queryException(RUUID queryRUuid, RQueryException exceptionThrown) throws TException {
              sendError(RUuidUtil.toUuid(queryRUuid), exceptionThrown);
            }

            @Override
            public void partialUpdate(RUUID queryRUuid, RResultTable partialResult, short percentComplete)
                throws TException {
              sendResult(RUuidUtil.toUuid(queryRUuid), partialResult, percentComplete, false);
            }
          });
    }
  }

  private void sendError(UUID queryUuid, RQueryException exceptionThrown) {
    QueryResultRegistry.unregister(queryUuid);
    JsonQueryExceptionPayload res = new JsonQueryExceptionPayload();
    res.setText(exceptionThrown.getMessage());
    try {
      String resString = new JsonPayloadSerializer().serialize(res);
      getWebsocketSession().getAsyncRemote().sendText(resString);
      getWebsocketSession().close();
    } catch (IOException e) {
      System.out.println("Could not close session: " + e);
    } catch (JsonPayloadSerializerException e) {
      System.err.println("Could not serialize result: " + e);
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
      sendError(queryUuid, e);
      throw new RuntimeException("Bad query");
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

  @Override
  public String getPayloadType() {
    return PAYLOAD_TYPE;
  }

}
