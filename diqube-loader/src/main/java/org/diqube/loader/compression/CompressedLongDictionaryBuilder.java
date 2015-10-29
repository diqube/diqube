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
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.diqube.data.types.lng.array.BitEfficientLongArray;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.array.ReferenceBasedLongArray;
import org.diqube.data.types.lng.array.RunLengthLongArray;
import org.diqube.data.types.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.EmptyLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.loader.compression.CompressedLongArrayBuilder.BitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongArrayBuilder.ReferenceAndBitEfficientCompressionStrategy;
import org.diqube.util.Pair;

/**
 * Builds compressed {@link LongDictionary}s.
 * 
 * <p>
 * It currently builds {@link ArrayCompressedLongDictionary} objects with the better of two compressions:
 * 
 * <ul>
 * <li>A plain {@link BitEfficientLongArray}</li>
 * <li>A {@link ReferenceBasedLongArray} with a {@link BitEfficientLongArray} inside.</li>
 * </ul>
 * 
 * These compressions are fine to be used with dictionaries. {@link BitEfficientLongArray} use a special case for
 * {@link Long#MIN_VALUE} with which the {@link BitEfficientLongArray#get(int)} method might degenerate to a O(log m)
 * with m being the number of MIN_VALUES in the array. As dictionaries though have each value only once and therefore
 * {@link Long#MIN_VALUE} is contained in the array at most once, we can assume that log m is actually constant -
 * therefore the array-access complexity in both compression scenarios is constant. We therefore can hold that the
 * access to the dictionary itself is at most logarithmic. As counter example: It would not be meaningful to use
 * {@link RunLengthLongArray}s in dictionaries, as their get method is linear already and we would end up having worst
 * access to the dictionary of O(n log m).
 *
 * TODO #83: Extract super-interface.
 * 
 * @author Bastian Gloeckle
 */
public class CompressedLongDictionaryBuilder {
  private NavigableMap<Long, Long> entityMap;

  private String name;

  /**
   * @param entityMap
   *          From decompressed long value to temporary IDs that has been assigned already.
   */
  public CompressedLongDictionaryBuilder fromEntityMap(NavigableMap<Long, Long> entityMap) {
    this.entityMap = entityMap;
    return this;
  }

  public CompressedLongDictionaryBuilder withDictionaryName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Compresses the values and builds a new {@link LongDictionary} from them.
   * <p>
   * In addition to the new {@link LongDictionary}, this method returns a map that maps the temporary value IDs (as
   * provided in the map in {@link #fromEntityMap(NavigableMap)}) to the final IDs assigned by this builder. That map
   * will contain an entry only, if the ID of a specific value was actually changed by this builder.
   * 
   * @return A {@link Pair} of the newly built {@link LongDictionary} and the ID map (from temporary IDs to final IDs,
   *         containing the tuples where the ID was actually changed).
   */
  public Pair<LongDictionary<?>, Map<Long, Long>> build() {
    if (entityMap.size() == 0) {
      return new Pair<>(new EmptyLongDictionary(), new HashMap<>());
    } else if (entityMap.size() == 1) {
      Entry<Long, Long> entry = entityMap.entrySet().iterator().next();
      LongDictionary<?> dict = new ConstantLongDictionary(entry.getKey(), entry.getValue());
      return new Pair<>(dict, new HashMap<>());
    }

    Map<Long, Long> idMap = new HashMap<Long, Long>();

    long[] uncompressed = new long[entityMap.size()];
    Iterator<Entry<Long, Long>> entryIt = entityMap.entrySet().iterator();
    for (int i = 0; i < uncompressed.length; i++) {
      Entry<Long, Long> entry = entryIt.next();
      uncompressed[i] = entry.getKey();

      if (i != entry.getValue()) {
        idMap.put(entry.getValue(), (long) i);
      }
    }

    @SuppressWarnings("unchecked")
    CompressedLongArrayBuilder compressedBuilder =
        new CompressedLongArrayBuilder().withLogName(name).withValues(uncompressed)
            .withStrategies(BitEfficientCompressionStrategy.class, ReferenceAndBitEfficientCompressionStrategy.class);

    CompressedLongArray<?> compressedArray = compressedBuilder.build();
    LongDictionary<?> dictRes = new ArrayCompressedLongDictionary(compressedArray);
    return new Pair<>(dictRes, idMap);
  }

}
