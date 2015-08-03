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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.diqube.data.colshard.ColumnShard;
import org.diqube.execution.consumers.AbstractThreadedColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

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
    private final ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>> EMPTY_VALUE = new ConcurrentHashMap<>();

    @Override
    protected void allSourcesAreDone() {
      ResolveValuesStep.this.sourcesAreEmpty.set(true);
    }

    @Override
    protected void doConsume(ExecutionEnvironment env, String colName, Map<Long, Long> rowIdToColumnDictId) {
      // acquire read lock, because multiple threads might access the following code, but none might access the
      // "writeLock" code in the execute() method.
      rowIdReadWriteLock.readLock().lock();
      try {
        // put a single column name string object into the map
        inputColsAndRows.putIfAbsent(colName, EMPTY_VALUE);
        // fetch that single key string (which is equal to all threads!)
        colName = inputColsAndRows.floorKey(colName);

        // .. now we can use that string object to sync upon - the following code will only be executed by one thread
        // simultaneously for a single colName.
        synchronized (colName) {
          logger.debug("Integrating column value IDs for col {} from {} for rowIds (limit) {}", colName, env,
              Iterables.limit(rowIdToColumnDictId.keySet(), 100));

          // prepare new value map.
          ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>> newRowIdToColValueId =
              new ConcurrentHashMap<>(inputColsAndRows.get(colName));

          // for each of the input rowId/columnValueId pairs check if there is a newer version available already. If
          // not, put the new value!
          rowIdToColumnDictId.entrySet().forEach(new Consumer<Entry<Long, Long>>() {
            @Override
            public void accept(Entry<Long, Long> newEntry) {
              newRowIdToColValueId.merge( //
                  newEntry.getKey(), // rowId of entry to inspect
                  new Pair<>(env, newEntry.getValue()), // use this as new value
                  new BiFunction<Pair<ExecutionEnvironment, Long>, Pair<ExecutionEnvironment, Long>, Pair<ExecutionEnvironment, Long>>() {

                @Override
                public Pair<ExecutionEnvironment, Long> apply(Pair<ExecutionEnvironment, Long> currentValue,
                    Pair<ExecutionEnvironment, Long> newValue) {
                  ExecutionEnvironment currentEnv = currentValue.getLeft();
                  ExecutionEnvironment newEnv = newValue.getLeft();

                  if (!(currentEnv instanceof VersionedExecutionEnvironment))
                    return currentValue;

                  if (!(newEnv instanceof VersionedExecutionEnvironment))
                    return newValue;

                  if (((VersionedExecutionEnvironment) currentEnv)
                      .getVersion() < ((VersionedExecutionEnvironment) newEnv).getVersion())
                    return newValue;
                  return currentValue;
                }
              });
            }
          });

          // be sure to use the exactly same string object here again, as this might be in sync-use in other threads
          // already.
          inputColsAndRows.put(colName, newRowIdToColValueId);
        }
      } finally {
        rowIdReadWriteLock.readLock().unlock();
      }
    }
  };

  /**
   * Map from colName to map from rowId to pair containing the column Value ID and the Env to resolve the value from.
   * The Env and the col value ID of course have to be the newest ones.
   */
  private ConcurrentNavigableMap<String, ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>>> inputColsAndRows =
      new ConcurrentSkipListMap<>();

  private ReadWriteLock rowIdReadWriteLock = new ReentrantReadWriteLock();

  public ResolveValuesStep(int stepId, QueryRegistry queryRegistry) {
    super(stepId, queryRegistry);
  }

  @Override
  public void execute() {
    rowIdReadWriteLock.writeLock().lock();
    ConcurrentNavigableMap<String, ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>>> activeColsAndRows;
    try {
      activeColsAndRows = inputColsAndRows;
      inputColsAndRows = new ConcurrentSkipListMap<>();

      if (sourcesAreEmpty.get() && activeColsAndRows.isEmpty() && inputColsAndRows.isEmpty()) {
        // there won't be any input at all. Stop processing.
        forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
        doneProcessing();
        return;
      }
    } finally {
      rowIdReadWriteLock.writeLock().unlock();
    }

    if (activeColsAndRows.size() > 0) {
      logger.debug("Starting to resolve values...");
      QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();
      Map<String, Map<Long, Object>> valuesPerColumn = activeColsAndRows.entrySet().stream() //
          .parallel().flatMap( //
              new Function<Entry<String, ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>>>, Stream<Triple<String, Long, Object>>>() {
                @Override
                public Stream<Triple<String, Long, Object>> apply(
                    Entry<String, ConcurrentMap<Long, Pair<ExecutionEnvironment, Long>>> e) {
                  QueryUuid.setCurrentThreadState(uuidState);
                  try {
                    String colName = e.getKey();

                    List<Triple<String, Long, Object>> res = new ArrayList<>();

                    // group by ExecutionEnvs and columnValueIds, so we do not have to decompress specific colValueIds
                    // multiple times
                    Map<ExecutionEnvironment, SortedMap<Long, List<Long>>> envToColumnValueIdToRowId = new HashMap<>();

                    for (Entry<Long, Pair<ExecutionEnvironment, Long>> rowIdColValueIdEntry : e.getValue().entrySet()) {
                      Long rowId = rowIdColValueIdEntry.getKey();
                      Long columnValueId = rowIdColValueIdEntry.getValue().getRight();
                      ExecutionEnvironment env = rowIdColValueIdEntry.getValue().getLeft();

                      if (!envToColumnValueIdToRowId.containsKey(env))
                        envToColumnValueIdToRowId.put(env, new TreeMap<>());

                      if (!envToColumnValueIdToRowId.get(env).containsKey(columnValueId))
                        envToColumnValueIdToRowId.get(env).put(columnValueId, new ArrayList<>());
                      envToColumnValueIdToRowId.get(env).get(columnValueId).add(rowId);
                    }

                    for (ExecutionEnvironment env : envToColumnValueIdToRowId.keySet()) {
                      SortedMap<Long, List<Long>> columnValueIdToRowId = envToColumnValueIdToRowId.get(env);
                      Long[] sortedColumnValueIds =
                          columnValueIdToRowId.keySet().toArray(new Long[columnValueIdToRowId.keySet().size()]);

                      ColumnShard columnShard = env.getColumnShard(colName);
                      Object[] values = columnShard.getColumnShardDictionary().decompressValues(sortedColumnValueIds);

                      for (int i = 0; i < sortedColumnValueIds.length; i++) {
                        Long columnValueId = sortedColumnValueIds[i];
                        Object value = values[i];

                        for (Long rowId : columnValueIdToRowId.get(columnValueId))
                          res.add(new Triple<>(colName, rowId, value));
                      }
                    }

                    return res.stream();
                  } finally {
                    QueryUuid.clearCurrent();
                  }
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

      QueryUuid.setCurrentThreadState(uuidState);

      for (String colName : valuesPerColumn.keySet()) {
        logger.trace("Resolved values, sending them out now (limit): {}, {}", colName,
            Iterables.limit(valuesPerColumn.get(colName).entrySet(), 10));
        forEachOutputConsumerOfType(ColumnValueConsumer.class, c -> c.consume(colName, valuesPerColumn.get(colName)));
      }
    }

    if (sourcesAreEmpty.get() && inputColsAndRows.isEmpty()) {
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
