package org.diqube.listeners;

import org.diqube.context.AutoInstatiate;

/**
 * Listener that gets informed when tables are un-/loaded.
 * 
 * All implementing classes need to have a bean inside the context (= need to have the {@link AutoInstatiate}
 * annotation).
 *
 * @author Bastian Gloeckle
 */
public interface TableLoadListener {
  /**
   * A new TableShard of a table has been loaded and is available in the TableRegistry.
   * 
   * @param tableName
   *          Name of the table the new shard belongs to.
   */
  public void tableShardLoaded(String tableName);

  /**
   * A TableShard of a table has been unloaded and is no longer available in the TableRegistry.
   * 
   * @param tableName
   *          Name of the table the shard belonged to.
   * @param nodeStillContainsOtherShard
   *          <code>true</code> in case there are still other shards of the table available in the TableRegistry.
   */
  public void tableShardUnloaded(String tableName, boolean nodeStillContainsOtherShard);
}
