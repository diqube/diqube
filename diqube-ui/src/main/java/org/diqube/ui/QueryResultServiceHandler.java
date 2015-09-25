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

import java.util.UUID;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryResultService.Iface;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@link QueryResultService}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryResultServiceHandler implements Iface {

  private static final Logger logger = LoggerFactory.getLogger(QueryResultServiceHandler.class);

  @Inject
  private UiQueryRegistry queryResultRegistry;

  @Override
  public void partialUpdate(RUUID queryRUuid, RResultTable partialResult, short percentComplete) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.debug("Received partial update for {}, percent {}: {}", queryUuid, percentComplete, partialResult);

    QueryResultService.Iface handler = queryResultRegistry.getHandler(queryUuid);
    if (handler != null)
      handler.partialUpdate(queryRUuid, partialResult, percentComplete);
  }

  @Override
  public void queryResults(RUUID queryRUuid, RResultTable finalResult) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.debug("Received FINAL update for {}: {}", queryUuid, finalResult);

    QueryResultService.Iface handler = queryResultRegistry.getHandler(queryUuid);
    if (handler != null)
      handler.queryResults(queryRUuid, finalResult);
  }

  @Override
  public void queryException(RUUID queryRUuid, RQueryException exceptionThrown) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.debug("Received EXCEPTION {}: {}", queryUuid, exceptionThrown.getMessage());

    QueryResultService.Iface handler = queryResultRegistry.getHandler(queryUuid);
    if (handler != null)
      handler.queryException(queryRUuid, exceptionThrown);
  }

  @Override
  public void queryStatistics(RUUID queryRuuid, RQueryStatistics stats) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(queryRuuid);
    logger.debug("Received STATS {}: {}", queryUuid, stats.toString());

    QueryResultService.Iface handler = queryResultRegistry.getHandler(queryUuid);
    if (handler != null)
      handler.queryStatistics(queryRuuid, stats);
  }

}
