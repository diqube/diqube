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

import org.apache.thrift.TException;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;
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
  public String diql;

  public PlainQueryJsonCommand() {
    super();
  }

  public PlainQueryJsonCommand(String diql) {
    this.diql = diql;
  }

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
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

      private void sendResult(UUID queryUuid, RResultTable finalResult, int percentComplete) {
        TableJsonResult res = new TableJsonResult();
        res.setColumnNames(finalResult.getColumnNames());
        List<List<Object>> rows = new ArrayList<>();
        if (finalResult.isSetRows()) { // if result table is empty, there are no rows.
          for (List<RValue> incomingResultRow : finalResult.getRows()) {
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
