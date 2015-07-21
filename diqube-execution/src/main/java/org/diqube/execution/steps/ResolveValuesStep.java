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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.diqube.execution.consumers.AbstractThreadedColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory.getLogger(ResolveValuesStep.class);

  private AtomicBoolean sourcesAreEmpty = new AtomicBoolean(false);

  private AbstractThreadedColumnDictIdConsumer columnDictIdConsumer = new AbstractThreadedColumnDictIdConsumer(this) {
    private Object newEntrySync = new Object();

    @Override
    protected void allSourcesAreDone() {
      ResolveValuesStep.this.sourcesAreEmpty.set(true);
    }

    @Override
    protected void doConsume(ExecutionEnvironment env, String colName, Map<Long, Long> rowIdToColumnDictId) {
      Pair<ExecutionEnvironment, String> keyPair = new Pair<>(env, colName);
      rowIdReadWriteLock.readLock().lock();
      try {
        if (!rowIds.containsKey(keyPair)) {
          synchronized (newEntrySync) {
            if (!rowIds.containsKey(keyPair))
              rowIds.put(keyPair, new ConcurrentHashMap<>());
          }
        }

        rowIds.get(keyPair).putAll(rowIdToColumnDictId);
      } finally {
        rowIdReadWriteLock.readLock().unlock();
      }
    }
  };

  private ConcurrentMap<Pair<ExecutionEnvironment, String>, ConcurrentMap<Long, Long>> rowIds =
      new ConcurrentHashMap<>();

  private ReadWriteLock rowIdReadWriteLock = new ReentrantReadWriteLock();

  public ResolveValuesStep(int stepId)

  {
    super(stepId);
  }

  @Override
  public void execute() {
    rowIdReadWriteLock.writeLock().lock();
    ConcurrentMap<Pair<ExecutionEnvironment, String>, ConcurrentMap<Long, Long>> activeColsAndRows;
    try {
      activeColsAndRows = rowIds;
      rowIds = new ConcurrentHashMap<>();
    } finally {
      rowIdReadWriteLock.writeLock().unlock();
    }

    if (activeColsAndRows.size() > 0) {
      Map<String, Map<Long, Object>> valuesPerColumn = activeColsAndRows.entrySet().stream() //
          .parallel().flatMap( //
              new Function<Entry<Pair<ExecutionEnvironment, String>, ConcurrentMap<Long, Long>>, Stream<Triple<String, Long, Object>>>() {
                @Override
                public Stream<Triple<String, Long, Object>> apply(
                    Entry<Pair<ExecutionEnvironment, String>, ConcurrentMap<Long, Long>> e) {
                  ExecutionEnvironment env = e.getKey().getLeft();
                  String colName = e.getKey().getRight();

                  List<Triple<String, Long, Object>> res = new ArrayList<>();

                  // group columnValueIds, so we do not have to decompress specific colValueIds multiple times
                  SortedMap<Long, List<Long>> columnValueIdToRowId = new TreeMap<>();

                  for (Entry<Long, Long> rowIdColValueIdEntry : e.getValue().entrySet()) {
                    Long rowId = rowIdColValueIdEntry.getKey();
                    Long columnValueId = rowIdColValueIdEntry.getValue();
                    if (!columnValueIdToRowId.containsKey(columnValueId))
                      columnValueIdToRowId.put(columnValueId, new ArrayList<>());
                    columnValueIdToRowId.get(columnValueId).add(rowId);
                  }

                  Long[] sortedColumnValueIds =
                      columnValueIdToRowId.keySet().toArray(new Long[columnValueIdToRowId.keySet().size()]);

                  Object[] values =
                      env.getColumnShard(colName).getColumnShardDictionary().decompressValues(sortedColumnValueIds);

                  for (int i = 0; i < sortedColumnValueIds.length; i++) {
                    Long columnValueId = sortedColumnValueIds[i];
                    Object value = values[i];

                    for (Long rowId : columnValueIdToRowId.get(columnValueId))
                      res.add(new Triple<>(colName, rowId, value));
                  }

                  return res.stream();
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

      logger.trace("Resolved values, sending them out now");

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
