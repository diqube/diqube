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
package org.diqube.ui.websocket.request.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.apache.thrift.TException;
import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.RValue;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.thrift.base.util.RValueUtil;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.result.StatsJsonResult;
import org.diqube.ui.websocket.result.TableJsonResult;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link AsyncJsonCommand} that takes a diql query string and starts executing that query on the available cluster
 * nodes.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link TableJsonResult} (multiple)
 * <li>{@link StatsJsonResult}
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = PlainQueryJsonCommand.NAME)
public class PlainQueryJsonCommand implements AsyncJsonCommand {

  public static final String NAME = "plainQuery";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String diql;

  public PlainQueryJsonCommand() {
    super();
  }

  public PlainQueryJsonCommand(String diql) {
    this.diql = diql;
  }

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    clusterInteraction.executeDiqlQuery(diql, new QueryResultService.Iface() {
      @Override
      public void queryStatistics(RUUID queryRuuid, RQueryStatistics stats) throws TException {
        StatsJsonResult statsPayload = new StatsJsonResult();
        statsPayload.loadFromQueryStatRes(stats);

        // TODO #52: handle case where we do not receive STATS - who closes the websocket?
        resultHandler.sendData(statsPayload);
        resultHandler.sendDone();
      }

      @Override
      public void queryResults(RUUID queryRUuid, RResultTable finalResult) throws TException {
        sendResult(RUuidUtil.toUuid(queryRUuid), finalResult, 100);
      }

      private void sendResult(UUID queryUuid, RResultTable currentResult, int percentComplete) {
        TableJsonResult res = new TableJsonResult();
        res.setColumnNames(currentResult.getColumnNames());
        res.setColumnRequests(currentResult.getColumnRequests());
        List<List<Object>> rows = new ArrayList<>();
        if (currentResult.isSetRows()) { // if result table is empty, there are no rows.
          for (List<RValue> incomingResultRow : currentResult.getRows()) {
            List<Object> row =
                incomingResultRow.stream().map(rValue -> RValueUtil.createValue(rValue)).collect(Collectors.toList());
            rows.add(row);
          }
          res.setRows(rows);
        }
        res.setPercentComplete((short) percentComplete);

        resultHandler.sendData(res);
      }

      @Override
      public void queryException(RUUID queryRUuid, RQueryException exceptionThrown) throws TException {
        sendError(RUuidUtil.toUuid(queryRUuid), exceptionThrown);
      }

      @Override
      public void partialUpdate(RUUID queryRUuid, RResultTable partialResult, short percentComplete) throws TException {
        sendResult(RUuidUtil.toUuid(queryRUuid), partialResult, percentComplete);
      }

      private void sendError(UUID queryUuid, RQueryException exceptionThrown) {
        resultHandler.sendException(exceptionThrown);
      }
    });
  }

  @Override
  public void cancel(CommandClusterInteraction clusterInteraction) {
    clusterInteraction.cancelQuery();
  }

}
