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
package org.diqube.server.thrift;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.thrift.server.TThreadedSelectorServer;
import org.diqube.listeners.ServingListener;

/**
 * A simple sub-class of {@link TThreadedSelectorServer} that enforces the selectorThreads to get a nice name.
 *
 * <p>
 * Unfortunately, there is no way to get to the AcceptThread - that thread therefore has a generic name.
 * 
 * @author Bastian Gloeckle
 */
public class ThriftServer extends TThreadedSelectorServer {

  private String selectorThreadNameFormat;
  private List<ServingListener> servingListeners;

  /**
   * @param args
   *          See {@link TThreadedSelectorServer#TThreadedSelectorServer(Args)}
   * @param selectorThreadNameFormat
   *          a {@link String#format(String, Object...)}-compatible format String, to which a unique integer (0, 1,
   *          etc.) will be supplied as the single parameter. This integer will be unique to the built instance of the
   *          ThreadFactory and will be assigned sequentially. For example, {@code "rpc-pool-%d"} will generate thread
   *          names like {@code "rpc-pool-0"}, {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
   */
  public ThriftServer(Args args, String selectorThreadNameFormat, List<ServingListener> servingListeners) {
    super(args);
    this.selectorThreadNameFormat = selectorThreadNameFormat;
    this.servingListeners = servingListeners;
  }

  @Override
  protected SelectorThreadLoadBalancer createSelectorThreadLoadBalancer(Collection<? extends SelectorThread> threads) {
    // It is far from perfect to use this method to set the thread names, but unfortunately, super.selectorThreads is
    // private and we cannot access it otherwise :(
    int i = 0;
    for (Iterator<? extends SelectorThread> it = threads.iterator(); it.hasNext();)
      it.next().setName(String.format(selectorThreadNameFormat, i++));

    return super.createSelectorThreadLoadBalancer(threads);
  }

  @Override
  protected void setServing(boolean serving) {
    super.setServing(serving);

    if (serving)
      servingListeners.forEach(l -> l.localServerStartedServing());
    else
      servingListeners.forEach(l -> l.localServerStoppedServing());
  }
}
