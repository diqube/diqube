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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.diqube.execution.consumers.AbstractThreadedColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.util.Pair;
import org.diqube.util.Triple;

/**
 * A step that takes the output of a {@link ResolveColumnDictIdsStep} and transforms the column value IDs into final
 * values by looking them up in the column dictionaries.
 *
 * <p>
 * This takes the order in which the inputs are providing new values into account. This is due to the fact that
 * {@link ResolveColumnDictIdsStep} might be based on a {@link ColumnVersionBuiltConsumer} where the values of a
 * specific column/row combination might change during the execution of the pipeline. The later the dict IDs are
 * resolved, the better the value of the column is therefore, so later calls need to overwrite the results of earlier
 * ones.
 *
 * <p>
 * Input: one or multiple {@link ColumnDictIdConsumer}<br>
 * Output: {@link ColumnValueConsumer}s
 *
 * @author Bastian Gloeckle
 */
public class ResolveValuesStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean sourcesAreEmpty = new AtomicBoolean(false);

  private AbstractThreadedColumnDictIdConsumer columnDictIdConsumer = new AbstractThreadedColumnDictIdConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      ResolveValuesStep.this.sourcesAreEmpty.set(true);
    }

    @Override
    protected void doConsume(ExecutionEnvironment env, String colName, Map<Long, Long> rowIdToColumnDictId) {
      for (Entry<Long, Long> entry : rowIdToColumnDictId.entrySet()) {
        ResolveValuesStep.this.rowIds.add(new Triple<Pair<ExecutionEnvironment, String>, Long, Long>(
            new Pair<ExecutionEnvironment, String>(env, colName), entry.getKey(), entry.getValue()));
      }
    }
  };

  private ConcurrentLinkedDeque<Triple<Pair<ExecutionEnvironment, String>, Long, Long>> rowIds =
      new ConcurrentLinkedDeque<>();

  public ResolveValuesStep(int stepId) {
    super(stepId);
  }

  @Override
  public void execute() {
    List<Triple<Pair<ExecutionEnvironment, String>, Long, Long>> activeColsAndRows = new ArrayList<>();
    Triple<Pair<ExecutionEnvironment, String>, Long, Long> entry;
    Set<String> colNames = new HashSet<>();
    while ((entry = rowIds.poll()) != null) {
      activeColsAndRows.add(entry);
      colNames.add(entry.getLeft().getRight());
    }

    if (activeColsAndRows.size() > 0) {
      Map<String, Map<Long, Object>> valuesPerColumn = activeColsAndRows.stream() //
          .sequential() // make sure this is executed sequential - we want later Triples in input to overwrite values of
                        // earlier Triples.
          .map( //
              new Function<Triple<Pair<ExecutionEnvironment, String>, Long, Long>, Triple<String, Long, Object>>() {
                // map to triple containing value
                @Override
                public Triple<String, Long, Object> apply(Triple<Pair<ExecutionEnvironment, String>, Long, Long> t) {
                  ExecutionEnvironment env = t.getLeft().getLeft();
                  String colName = t.getLeft().getRight();
                  Long rowId = t.getMiddle();
                  Long columnValueId = t.getRight();

                  // TODO #8 decompress multiple values at once
                  Object value = env.getColumnShard(colName).getColumnShardDictionary().decompressValue(columnValueId);

                  return new Triple<>(colName, rowId, value);
                }

              })
          .collect(() -> new HashMap<String, Map<Long, Object>>(), (map, triple) -> {
            String colName = triple.getLeft();
            Long rowId = triple.getMiddle();
            Object value = triple.getRight();
            if (!map.containsKey(colName))
              map.put(colName, new HashMap<>());
            map.get(colName).put(rowId, value);
          } , (map1, map2) -> {
            for (String colName : map2.keySet()) {
              if (!map1.containsKey(colName))
                map1.put(colName, new HashMap<>());
              map1.get(colName).putAll(map2.get(colName));
            }
          });

      for (String colName : valuesPerColumn.keySet())
        forEachOutputConsumerOfType(ColumnValueConsumer.class, c -> c.consume(colName, valuesPerColumn.get(colName)));
    }

    if (sourcesAreEmpty.get() && rowIds.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  public List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { columnDictIdConsumer });
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof ColumnValueConsumer))
      throw new IllegalArgumentException("Only ColumnValueConsumer supported!");
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // intentionally empty, as we do not track wire-status nicely here, as we wire our consumer multiple times.
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

}
