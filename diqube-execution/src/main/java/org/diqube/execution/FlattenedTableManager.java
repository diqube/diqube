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
package org.diqube.execution;

import java.util.Deque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.util.Pair;

/**
 * Manages various {@link FlattenedTable}s that are available locally.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenedTableManager {
  private Map<Pair<String, String>, Deque<Pair<UUID, FlattenedTable>>> tables = new ConcurrentHashMap<>();

  public void registerFlattenedTableVersion(UUID versionId, FlattenedTable flattenedTable, String origTableName,
      String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<Pair<UUID, FlattenedTable>> deque = tables.get(keyPair);

    Pair<UUID, FlattenedTable> newEntryPair = new Pair<>(versionId, flattenedTable);

    if (deque == null) {
      deque = new ConcurrentLinkedDeque<>();
      deque.addFirst(newEntryPair);
      Deque<Pair<UUID, FlattenedTable>> finalDeque = deque;

      tables.merge(keyPair, deque, (oldDeque, newDeque) -> {
        oldDeque.addAll(finalDeque);
        return oldDeque;
      });
    } else
      deque.addFirst(newEntryPair);
  }

  public Pair<UUID, FlattenedTable> getNewestFlattenedTableVersion(String origTableName, String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<Pair<UUID, FlattenedTable>> deque = tables.get(keyPair);
    if (deque == null)
      return null;

    try {
      return deque.getFirst();
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
