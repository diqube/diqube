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

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread that monitors a {@link ConcurrentLinkedDeque} for additions of row-wise data, which are then processed by this
 * thread. They will be transposed (= rows become columns and columns become rows) and a
 * {@link ColumnValuesReadyCallback} will be fed the results.
 * 
 * <p>
 * This thread will itself monitor the deque and use a separate {@link ExecutorService} to actually process the data as
 * soon as new data is found. This thread therefore creates additional worker threads.
 * 
 * @author Bastian Gloeckle
 */
public class TransposeThread extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(TransposeThread.class);

  public static final int GRACEFUL_SHUTDOWN_PERIOD_SECONDS = 30;

  private static final int MAX_TRANSPOSE_WORKERS = 10;

  private ConcurrentLinkedDeque<String[][]> deque;
  private ExecutorService executorService;
  private Function<Integer, Long> provideRowIdsFn;
  private String[] columnNames;
  private TransposeThread.ColumnValuesReadyCallback columnValuesReadyCallbacks;
  private volatile boolean wasGoodShutdown;
  private String shutdownExceptionMessage = null;

  /** Number of batches that we successfully worked on - either processed successfully or handled its exceptions */
  private AtomicInteger batchesWorkedOn = new AtomicInteger(0);
  /** Number of batches that started o hand over to the {@link ExecutorService}. */
  private int batchesReceived = 0;
  /** sync/notify object for batches - whenever {@link #batchesWorkedOn} is changed this should be notified. */
  private Object batchNotify = new Object();

  private boolean dequeFilled = false;

  private Object resultNotifyObject;

  private boolean transposeDone = false;

  private String tableName;

  /**
   * Create new thread.
   * 
   * @param deque
   *          The Deque that will be watched for new row-wise data (String[row][column] = value). Column indices are
   *          mapped to column names by columnNames parameter.
   * @param provideRowIdsFn
   *          A {@link Function} that will reserve a specific amount of unique Row IDs. The function returns the lowest
   *          of the reserved rowIDs.
   * @param columnValuesReadyCallbacks
   *          As soon as data has been transformed to a columnar format, this callback will be called in one of the
   *          child threads of this thread. Here, the initiator of this thread can then process the data further.
   * @param columnNames
   *          The column names of the columns.
   * @param tableName
   *          The name of the table that is about to be created.
   */
  public TransposeThread(ConcurrentLinkedDeque<String[][]> deque, Function<Integer, Long> provideRowIdsFn,
      TransposeThread.ColumnValuesReadyCallback columnValuesReadyCallbacks, String[] columnNames, String tableName,
      ExecutorManager executorManager) {
    super("transpose-" + tableName);
    this.deque = deque;
    this.provideRowIdsFn = provideRowIdsFn;
    this.columnValuesReadyCallbacks = columnValuesReadyCallbacks;
    this.columnNames = columnNames;
    this.tableName = tableName;

    executorService = executorManager.newCachedThreadPoolWithMax("transpose-worker-" + tableName + "-%d",
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            wasGoodShutdown = false;
            shutdownExceptionMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.error("Exception while transposing data of table " + tableName, e);
            TransposeThread.this.interrupt();

            // exception case: We worked on a batch, although it turned out to be an exception.
            // Remember and wake up any threads that might be waiting on that batch...
            batchesWorkedOn.incrementAndGet();
            synchronized (batchNotify) {
              batchNotify.notifyAll();
            }
          }
        }, MAX_TRANSPOSE_WORKERS);
    wasGoodShutdown = true;
  }

  /**
   * Tell this thread that the {@link Deque} that was passed to the constructor now contains all input data.
   * 
   * AFter the transposing is done, the {@link Object#notifyAll()} method will be called on the given object.
   */
  public void inputDequeIsFilledNotifyWhenTransposed(Object notifyObject) {
    this.resultNotifyObject = notifyObject;
    dequeFilled = true;
    this.interrupt();
  }

  /**
   * Available after the thread has ended.
   * 
   * @return true if there were exceptions while shutting down or if the child {@link ExecutorService} had to be
   *         forcefully shutdown ({@link ExecutorService#shutdownNow()} has been called).
   */
  public boolean wasGoodShutdown() {
    return wasGoodShutdown;
  }

  /**
   * If after execution of this thread {@link #wasGoodShutdown()} is false, this getter might return additional
   * information on why the thread was not able to successfully process. The result value might be <code>null</code> in
   * which case there is no additional information.
   */
  public String getShutdownExceptionMessage() {
    return shutdownExceptionMessage;
  }

  /**
   * @return <code>true</code> if the transpose is done.
   */
  public boolean isTransposeDone() {
    return transposeDone;
  }

  @Override
  public void run() {
    logger.trace("New TransposeThread starts working...");
    try {
      while (!dequeFilled || !deque.isEmpty()) {
        if (deque.peek() != null) {
          for (Iterator<String[][]> it = deque.iterator(); it.hasNext();) {
            String[][] batch = it.next();
            logger.trace("Triggering batch with {} rows", batch.length);
            batchesReceived++;
            // Start processing in executorService. As we might call the execute method and the shutdown method (see
            // below) quickly after each other (e.g. if there is a fast call to #initiateGracefulShutdown()), we need
            // some additional synchronization. I'm not sure whether this is a bug in ThreadPoolExecutor or if it's
            // intended, but it
            // seems that in ThreadPoolExecutor#runWorker(Worker) there is one position where a new task is fetched
            // already, but the lock has not yet been acquired (beginning of the while loop, line 1127). Seems like if
            // we call the shutdown method right before the lock is acquired, the shutdown might succeed and the
            // tryTerminate method might even kill our new worker right away again, before it even started to process
            // anything. Or, even worse, it started to process, threw an exception which ends up in the
            // UncaughtExceptionHandler, but the update to #wasGoodShutdown (=false) is not recognized by other classes,
            // because this Thread has terminated and they checked the field already.
            // Therefore we make sure here, that we fully process all the batches that we passed on to the
            // executor service: We count the number of batches handed over to the executorService and the ones that we
            // processed (either successfully at the end of the run method, or if an exception is thrown in the
            // UncaughtExceptionHandler, see constructor).
            try {
              executorService.execute(new Runnable() {
                @Override
                public void run() {
                  logger.trace("Starting to transpose some values");
                  long baseRowId = provideRowIdsFn.apply(batch.length);

                  String[][] columns = new String[columnNames.length][];
                  for (int col = 0; col < columns.length; col++)
                    columns[col] = new String[batch.length];

                  for (int row = 0; row < batch.length; row++) {
                    for (int col = 0; col < columnNames.length; col++)
                      columns[col][row] = batch[row][col];
                  }

                  logger.trace("Transposed {} values of columns {} (firstRowId {}).", batch.length, columnNames,
                      baseRowId);

                  for (int col = 0; col < columns.length; col++)
                    columnValuesReadyCallbacks.columnValuesReady(columnNames[col], columns[col], baseRowId);

                  // if everything went well (= no exception), then we have fully worked on this batch. Remember that
                  // and
                  // wake up any threads that might be waiting.
                  batchesWorkedOn.incrementAndGet();
                  synchronized (batchNotify) {
                    batchNotify.notifyAll();
                  }
                }
              });
              it.remove();
            } catch (RejectedExecutionException e) {
              // swallow. Execution was rejected, because the executorService is full currently. We just retry next
              // time.
              logger.trace("Executor rejected to execute something. Will try again later...");
              batchesReceived--; // we will retry this batch!
              break;
            } catch (RuntimeException e) {
              wasGoodShutdown = false;
              shutdownExceptionMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
              logger.error("Exception while transposing data of table " + tableName, e);
              executorService.shutdownNow();
              return;
            }
          }
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          if (!dequeFilled) {
            logger.trace("TransposeThread interrupted, with deque not being filled completely. Shutting down.");
            wasGoodShutdown = false;
            executorService.shutdownNow();
            return;
          }
        }
      }
    } finally {
      logger.trace("TransposeThread starting shutdown, wasGoodShutdown={}", wasGoodShutdown);
      executorService.shutdown();

      // wait until we processed all of our batches, at max GRACEFUL_SHUTDOWN_PERIOD_SECONDS seconds.
      int batchesMissing = batchesReceived - batchesWorkedOn.get();
      while (batchesWorkedOn.get() != batchesReceived) {
        synchronized (batchNotify) {
          try {
            batchNotify.wait((GRACEFUL_SHUTDOWN_PERIOD_SECONDS / batchesMissing) * 1000);
          } catch (InterruptedException e) {
            wasGoodShutdown = false;
            break;
          }
        }
      }

      // All executor threads /should/ be stopped by now, but let's make absolutely sure....
      executorService.shutdownNow();

      logger.trace("TransposeThread shutdown completed, wasGoodShutdown={}", wasGoodShutdown);

      transposeDone = true;
      synchronized (resultNotifyObject) {
        resultNotifyObject.notifyAll();
      }
    }
  }

  public static interface ColumnValuesReadyCallback {
    /**
     * Column values have been computed for the given column.
     * 
     * @param baseRowId
     *          The rowId of the first value in the array. Consecutive values have increasing row IDs.
     */
    public void columnValuesReady(String colName, String[] values, long baseRowId);
  }
}