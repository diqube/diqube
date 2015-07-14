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
package org.diqube.loader.util;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.diqube.loader.LoadException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.threads.ExecutorManager;

/**
 * Helper for transposing row-wise data into columnar format and applying the transformation functions on the columnar
 * values (see {@link LoaderColumnInfo}) in a multi-threaded way.
 *
 * @author Bastian Gloeckle
 */
public class ParallelLoadAndTransposeHelper {
  private LoaderColumnInfo columnInfo;
  private ColumnShardBuilderManager columnBuilderManager;
  private String[] colNames;
  private String tableName;
  private ExecutorManager executorManager;

  /**
   * 
   * @param columnInfo
   *          Column Info about the columns that are being created.
   * @param columnBuilderManager
   *          The target {@link ColumnShardBuilderManager} where the values of the columns should be put into.
   * @param colNames
   *          Names of the columns, in the same ordering as the row-wise data will later provide the column values.
   * @param tableName
   *          The name of the table to be created.
   */
  public ParallelLoadAndTransposeHelper(ExecutorManager executorManager, LoaderColumnInfo columnInfo,
      ColumnShardBuilderManager columnBuilderManager, String[] colNames, String tableName) {
    this.executorManager = executorManager;
    this.columnInfo = columnInfo;
    this.columnBuilderManager = columnBuilderManager;
    this.colNames = colNames;
    this.tableName = tableName;
  }

  /**
   * Start a thread that will transpose the row-wise data which will be read by a custom rowWiseLoader and apply the
   * {@link LoaderColumnInfo#getFinalTransformFunc(String)} on those columnar values.
   * 
   * <p>
   * This method will take care of shutting down any created threads before returning. This method will return as soon
   * as the rowWiseLoader has returned and all of that data has been processed.
   * 
   * @param firstRowId
   *          the first row ID that should be given to the table shard being created.
   * @param rowWiseLoader
   *          This consumer will be called right after setting up the thread which will transpose the row-wise data.
   *          This consumer then needs to load row-wise data and store it into the {@link ConcurrentLinkedDeque} that is
   *          provided to the consumer as parameter. Each entry that is stored into the Deque can contain the data of
   *          multiple rows, each row containing the values for all columns (as defined by the columnNames parameter in
   *          the constructor!). The first index to the two-dimensional array is the row, the second is the value:
   *          String[row][column] = value.
   * @throws LoadException
   *           If anything goes wrong.
   */
  public void transpose(long firstRowId, Consumer<ConcurrentLinkedDeque<String[][]>> rowWiseLoader)
      throws LoadException {
    final ConcurrentLinkedDeque<String[][]> rowWiseData = new ConcurrentLinkedDeque<>();

    // Prepare a source of RowIDs.
    AtomicLong nextRowId = new AtomicLong(firstRowId);

    // This thread will continuously look at the rowWiseData deque and, if it finds new data, transform that to columnar
    // format and send the column values to the column builder manager.
    TransposeThread transposeThread = new TransposeThread( //
        rowWiseData, // read from this input
        l -> nextRowId.getAndAdd(l), // retrieve a set of new, unique rowIds
        (col, values, rowId) -> { // add results to the col Builders
          Object[] finalValues = columnInfo.getFinalTransformFunc(col).apply(values);
          columnBuilderManager.addValues(col, finalValues, rowId);
        } , colNames, tableName, executorManager);

    try {
      transposeThread.start();

      rowWiseLoader.accept(rowWiseData);
    } finally {
      // try to gracefully shut down the thread and wait for it.
      transposeThread.initiateGracefulShutdown();
      boolean interruptedException = false;
      try {
        transposeThread.join((TransposeThread.GRACEFUL_SHUTDOWN_PERIOD_SECONDS + 10) * 1000);
      } catch (InterruptedException e) {
        interruptedException = true;
      }
      // If the thread did not finish successfully, make sure to not continue processing the CSV.
      if (!transposeThread.wasGoodShutdown() || interruptedException)
        if (transposeThread.getShutdownExceptionMessage() != null)
          throw new LoadException(transposeThread.getShutdownExceptionMessage());
        else
          throw new LoadException("TransposeThread did not exit successfully. Was it interrupted?");
    }
  }
}
