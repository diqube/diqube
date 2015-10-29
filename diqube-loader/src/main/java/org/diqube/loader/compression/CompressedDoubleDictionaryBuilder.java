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
package org.diqube.loader.compression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;

import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.dbl.dict.FpcDoubleDictionary;
import org.diqube.data.types.dbl.dict.FpcPage;
import org.diqube.data.types.dbl.dict.FpcPage.State;
import org.diqube.util.Pair;

/**
 * Builds a compressed string dictionary out of a map that contains values and temporary ids.
 *
 * TODO #83: Extract super-interface.
 * 
 * @author Bastian Gloeckle
 */
public class CompressedDoubleDictionaryBuilder {
  public static final int PAGE_SIZE = 5_000;

  private NavigableMap<Double, Long> entityMap;

  /**
   * @param entityMap
   *          From decompressed string value to temporary Column Value IDs that have been assigned already.
   */
  public CompressedDoubleDictionaryBuilder fromEntityMap(NavigableMap<Double, Long> entityMap) {
    this.entityMap = entityMap;
    return this;
  }

  /**
   * Build the dictionary.
   * 
   * @return {@link Pair} containing the new {@link DoubleDictionary} and an ID change map (maps from temporary ID that
   *         was provided in {@link #fromEntityMap(Map)} to the final ID assigned in the resulting dict).
   */
  public Pair<DoubleDictionary<?>, Map<Long, Long>> build() {
    SortedSet<Double> keys = (SortedSet<Double>) entityMap.keySet();

    Map<Long, Long> idMap = new HashMap<>();
    long newId = 0;
    for (Double key : keys) {
      long thisId = newId++;
      if (entityMap.get(key) != thisId)
        idMap.put(entityMap.get(key), thisId);
    }

    NavigableMap<Long, FpcPage> pages = new TreeMap<>();

    long valuesLeft = newId;
    long firstId = 0L;
    Iterator<Double> keyIt = keys.iterator();
    State lastState = null;
    while (keyIt.hasNext()) {
      double[] valueArray;
      if (valuesLeft >= PAGE_SIZE)
        valueArray = new double[PAGE_SIZE];
      else
        valueArray = new double[(int) valuesLeft];

      for (int i = 0; i < valueArray.length; i++)
        valueArray[i] = keyIt.next();

      FpcPage newPage;
      if (lastState != null)
        newPage = new FpcPage(firstId);
      else
        newPage = new FpcPage(firstId, lastState);

      lastState = newPage.compress(valueArray);

      pages.put(firstId, newPage);

      valuesLeft -= valueArray.length;
      firstId += valueArray.length;
    }

    DoubleDictionary<?> res = new FpcDoubleDictionary(pages, keys.first(), keys.last());

    return new Pair<>(res, idMap);
  }
}
