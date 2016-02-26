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
package org.diqube.consensus;

import org.diqube.context.shutdown.ContextShutdownListener;
import org.diqube.util.CloseableNoException;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

/**
 * Provides client-side implementations of all {@link ConsensusStateMachine} interfaces: When the corresponding methods
 * are called, the operations are distributed in the consensus cluster.
 *
 * @author Bastian Gloeckle
 */
public interface ConsensusClient extends ContextShutdownListener {

  /**
   * Creates and returns an object implementing the given stateMachineInterface which will, when methods are called,
   * distribute those calls among the consensus cluster and only return when the {@link Command}/{@link Query} is
   * committed in the cluster.
   * 
   * <p>
   * The returned {@link ClosableProvider} can provide the created instance and must be closed after the client is not
   * needed any more!
   * 
   * <p>
   * Note that methods on the returned object are always executed synchronously and it may take an arbitrary time too
   * complete (e.g. if there is no consensus leader currently and a client session cannot be opened therefore). This is
   * especially true for nodes which are partitioned on the network from the majority of the cluster: When a method on
   * the returned object is called in such a case, it may take up until the network partition is resolved and the
   * cluster became fully available again until the methods return!
   * 
   * <p>
   * The returned object might throw {@link ConsensusStateMachineClientInterruptedException} on each method call, since
   * pure {@link InterruptedException}s cannot be thrown through the proxy. Be sure to catch them and throw the
   * encapsulated {@link InterruptedException}.
   * 
   * @param stateMachineInterface
   *          Interface which has the {@link ConsensusStateMachine} annotation.
   * @throws ConsensusClusterUnavailableException
   *           In case the consensus cluster seems to be not available currently.
   */
  public <T> ClosableProvider<T> getStateMachineClient(Class<T> stateMachineInterface)
      throws ConsensusClusterUnavailableException;

  /**
   * Provides a client to a consensus client. Needs to be {@link #close()}d correctly!
   */
  public static interface ClosableProvider<T> extends CloseableNoException {
    /**
     * See {@link ConsensusClient#getStateMachineClient(Class)}.
     */
    public T getClient();
  }

  /**
   * The consensus cluster is not available currently.
   */
  public static class ConsensusClusterUnavailableException extends Exception {
    private static final long serialVersionUID = 1L;

    public ConsensusClusterUnavailableException(String msg) {
      super(msg);
    }

    public ConsensusClusterUnavailableException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}