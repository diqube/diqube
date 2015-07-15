package org.diqube.listeners;

import org.diqube.context.AutoInstatiate;

/**
 * Listener for information if the local thrift server is serving or not.
 * 
 * All implementing classes need to have a bean in the context (=have the {@link AutoInstatiate} annotation).
 *
 * @author Bastian Gloeckle
 */
public interface ServingListener {
  /**
   * The local thrift server started serving, which means that our process is now reachable by other machines over the
   * network.
   */
  public void localServerStartedServing();

  /**
   * The local thrift server stopped serving, which means that our process is not reachable by anyone anymore.
   */
  public void localServerStoppedServing();
}
