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
package org.diqube.execution.steps;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.TableFlattenedConsumer;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.flatten.QueryMasterFlattenService;
import org.diqube.flatten.QueryMasterFlattenService.FlattenException;
import org.diqube.queries.QueryRegistry;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.util.Pair;

/**
 * Step which triggers flattening of a table on the query master node.
 * 
 * <p>
 * Input: none. <br/>
 * Output: {@link TableFlattenedConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class FlattenStep extends AbstractThreadedExecutablePlanStep {

  private String tableName;
  private String flattenBy;
  private QueryMasterFlattenService queryMasterFlattenService;

  public FlattenStep(int stepId, QueryRegistry queryRegistry, String tableName, String flattenBy,
      QueryMasterFlattenService queryMasterFlattenService) {
    super(stepId, queryRegistry);
    this.tableName = tableName;
    this.flattenBy = flattenBy;
    this.queryMasterFlattenService = queryMasterFlattenService;
  }

  @Override
  protected void execute() {
    Pair<UUID, List<RNodeAddress>> flattenRes;
    try {
      flattenRes = queryMasterFlattenService.flatten(tableName, flattenBy);
    } catch (FlattenException e) {
      throw new ExecutablePlanExecutionException("Could not flatten table: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      // quiet end
      doneProcessing();
      return;
    }

    if (flattenRes == null)
      throw new ExecutablePlanExecutionException("No cluster nodes serve table '" + tableName + "'");

    forEachOutputConsumerOfType(TableFlattenedConsumer.class,
        c -> c.tableFlattened(flattenRes.getLeft(), flattenRes.getRight()));
    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
    doneProcessing();
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>();
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof TableFlattenedConsumer) && !(consumer instanceof DoneConsumer))
      throw new IllegalArgumentException("Only TableFlattenedConsumer supported.");
  }

}
