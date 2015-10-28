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
package org.diqube.data.flatten;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TBase;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * A {@link ColumnPage} that contains only a single value.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class FlattenedConstantColumnPage implements ColumnPage {

  private String name;
  private ConstantLongDictionary colPageDict;
  private long firstRowId;
  private int rows;

  /* package */ FlattenedConstantColumnPage(String name, ConstantLongDictionary colPageDict, long firstRowId,
      int rows) {
    this.name = name;
    this.colPageDict = colPageDict;
    this.firstRowId = firstRowId;
    this.rows = rows;
  }

  @Override
  public LongDictionary<?> getColumnPageDict() {
    return colPageDict;
  }

  @Override
  public CompressedLongArray<?> getValues() {
    return new CompressedLongArray<TBase<?, ?>>() {
      @Override
      public boolean isSameValue() {
        return true;
      }

      @Override
      public int size() {
        return rows;
      }

      @Override
      public boolean isSorted() {
        return true;
      }

      @Override
      public long[] decompressedArray() {
        long[] res = new long[rows];
        Arrays.fill(res, 0L);
        return res;
      }

      @Override
      public long get(int index) throws ArrayIndexOutOfBoundsException {
        return 0; // constant 0, as 0 is the first ID of the constantLongDict
      }

      @Override
      public List<Long> getMultiple(List<Integer> sortedIndices) throws ArrayIndexOutOfBoundsException {
        List<Long> res = new ArrayList<>();
        for (int i = 0; i < sortedIndices.size(); i++)
          res.add(0L);
        return res;
      }

      @Override
      public long calculateApproximateSizeInBytes() {
        return 0;
      }

      @Override
      public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
          TBase<?, ?> target) throws SerializationException {
        throw new SerializationException("Cannot serialize flattened ColPage.");
      }

      @Override
      public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
          TBase<?, ?> source) throws DeserializationException {
        throw new DeserializationException("Cannot deserialize flattened ColPage.");
      }
    };
  }

  @Override
  public long getFirstRowId() {
    return firstRowId;
  }

  @Override
  public int size() {
    return rows;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this.
        colPageDict.calculateApproximateSizeInBytes() + 12 + name.length();
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, SColumnPage target)
      throws SerializationException {
    throw new SerializationException("Cannot serialize flattened ColPage.");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      SColumnPage source) throws DeserializationException {
    throw new DeserializationException("Cannot deserialize flattened ColPage.");
  }
}
