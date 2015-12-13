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

import java.time.Instant;

import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;

/**
 *
 * @author Bastian Gloeckle
 */
public class ConsensusUtil {
  /**
   * @return A {@link Commit} object which only contains an {@link Operation} - ONLY TO BE USED FOR ACCESSING OBJECTS
   *         CREATED WITH {@link ConsensusClient#getStateMachineClient(Class)}.
   */
  public static <T extends Operation<?>> Commit<T> localCommit(T obj) {
    return new LocalCommit<>(obj);
  }

  public static class LocalCommit<T extends Operation<?>> implements Commit<T> {
    private T operation;

    private LocalCommit(T operation) {
      this.operation = operation;
    }

    @Override
    public long index() {
      return 0;
    }

    @Override
    public Session session() {
      return null;
    }

    @Override
    public Instant time() {
      return null;
    }

    @Override
    public Class<T> type() {
      return null;
    }

    @Override
    public T operation() {
      return operation;
    }

    @Override
    public void clean() {
    }

    @Override
    public void close() {
    }

  }
}
