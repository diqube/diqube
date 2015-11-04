package org.diqube.execution.consumers;

import java.util.Collection;
import java.util.UUID;

import org.diqube.remote.base.thrift.RNodeAddress;

/**
 * A consumer that is informed as soon as a table is fully flattened.
 *
 * @author Bastian Gloeckle
 */
public interface TableFlattenedConsumer extends OverwritingConsumer {
  /**
   * The required table has been flattened fully and is ready for being queried.
   * 
   * @param flattenId
   *          The ID of the flattening - use this to query the table on the query remotes!
   * @param remoteNodes
   *          The addresses of the remote nodes where the table was flattened.
   */
  public void tableFlattened(UUID flattenId, Collection<RNodeAddress> remoteNodes);

}