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
package org.diqube.im.callback;

import java.time.Instant;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.consensus.DiqubeConsensusStateMachineClientInterruptedException;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.consensus.DiqubeCopycatClient.ClosableProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachine.Unregister;
import org.diqube.im.callback.IdentityCallbackRegistryStateMachineImplementation.IdentityCallbackRegistryListener;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans entries from {@link IdentityCallbackRegistryStateMachine} that are older than
 * {@link #CALLBACK_REGISTER_TIMEOUT_MIN}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class IdentityCallbackRegistryCleaner implements IdentityCallbackRegistryListener {
  private static final Logger logger = LoggerFactory.getLogger(IdentityCallbackRegistryCleaner.class);

  /** max. seconds after which {@link IdentityCallbackRegistryStateMachineImplementation} is polled and cleaned */
  public static final long MAX_POLL_SEC = 120;

  /** Timeout of a "registerCallback" call after which the callback is deregistered by this cleaner. */
  public static final long CALLBACK_REGISTER_TIMEOUT_MIN = 1 * 60; // 1h

  @Inject
  private IdentityCallbackRegistryStateMachineImplementation registry;

  @Inject
  private DiqubeCopycatClient consensusClient;

  private CheckThread thread;

  private NavigableMap<Long, Deque<RNodeAddress>> registeredTimes = new ConcurrentSkipListMap<>();

  @PostConstruct
  public void initialize() {
    thread = new CheckThread();
    thread.start();
  }

  @PreDestroy
  public void cleanup() {
    thread.interrupt();
  }

  @Override
  public void registered(RNodeAddress callbackNode, long registerTime) {
    registeredTimes.compute(registerTime, (key, oldValue) -> {
      Deque<RNodeAddress> res = new ConcurrentLinkedDeque<>();
      res.addLast(callbackNode);
      if (oldValue != null)
        res.addAll(oldValue);
      return res;
    });
  }

  @Override
  public void unregistered(RNodeAddress callbackNode, long lastRegisterTime) {
    registeredTimes.computeIfPresent(lastRegisterTime, (key, value) -> {
      Deque<RNodeAddress> res = new ConcurrentLinkedDeque<>();
      res.addAll(value);
      res.remove(callbackNode);
      if (res.isEmpty())
        return null;
      return res;
    });
  }

  private class CheckThread extends Thread {
    /* package */ CheckThread() {
      super("identity-callback-registry-cleaner");
    }

    @Override
    public void run() {
      long randomWaitDiff = ThreadLocalRandom.current().nextLong((MAX_POLL_SEC / 2) * 1_000L);
      while (true) {
        try {
          Thread.sleep((MAX_POLL_SEC / 2) * 1_000L + randomWaitDiff); // Random this that not all nodes execute this
                                                                      // simultaneously.
        } catch (InterruptedException e) {
          // exit quietly.
          return;
        }

        Set<RNodeAddress> callbackNodesToUnregister = new HashSet<>();
        long timeoutTime = System.currentTimeMillis() - CALLBACK_REGISTER_TIMEOUT_MIN * 60 * 1_000L;
        Map<Long, Deque<RNodeAddress>> timeouts = registeredTimes.headMap(timeoutTime);

        for (Entry<Long, Deque<RNodeAddress>> e : timeouts.entrySet()) {
          long registerTime = e.getKey();
          for (RNodeAddress callbackNode : e.getValue()) {
            Long currentRegisterTime = registry.getCurrentRegisterTime(callbackNode);
            // check if node is unregistered already
            if (currentRegisterTime == null)
              continue;

            // check if node was not re-registered already.
            if (currentRegisterTime > registerTime)
              continue;

            callbackNodesToUnregister.add(callbackNode);
          }
        }

        if (!callbackNodesToUnregister.isEmpty()) {
          logger.info(
              "Will unregister following {} nodes from receiving any calls to their IdentityCallbackService,"
                  + " because they did not re-register soon enough (registered before {}): {}",
              callbackNodesToUnregister.size(), Instant.ofEpochMilli(timeoutTime), callbackNodesToUnregister);

          try (ClosableProvider<IdentityCallbackRegistryStateMachine> p =
              consensusClient.getStateMachineClient(IdentityCallbackRegistryStateMachine.class)) {
            for (RNodeAddress addr : callbackNodesToUnregister)
              p.getClient().unregister(Unregister.local(addr));
          } catch (DiqubeConsensusStateMachineClientInterruptedException e) {
            // interrupted, exit quietly.
            return;
          } catch (RuntimeException e) {
            logger.warn("Could not exeucte unregister calls. Ignoring", e);
          }
        }
      }
    }

  }

}
