package org.diqube.listeners;

import org.diqube.context.AutoInstatiate;

/**
 * A listener for events of the ClusterManager.
 * 
 * All implementing classes need to be available in the context (= they need to have the {@link AutoInstatiate}
 * annotation).
 *
 * @author Bastian Gloeckle
 */
public interface ClusterManagerListener {

  /**
   * The ClusterManager is initialized and we're ready to interact with the cluster.
   */
  public void clusterInitialized();
}
