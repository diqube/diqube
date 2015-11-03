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

import java.util.Set;

import org.diqube.data.column.AdjustableColumnPage;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.util.DiqubeCollectors;

/**
 * A {@link ColumnPage} which is based on a delegatePage, but will "remove" multiple rowIds of that delegate in its
 * outside view.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class FlattenedIndexRemovingColumnPage implements AdjustableColumnPage {

  private String name;
  private LongDictionary<?> colPageDict;
  private long firstRowId;
  private IndexRemovingCompressedLongArray values;

  /* package */ FlattenedIndexRemovingColumnPage(String name, LongDictionary<?> colPageDict, ColumnPage delegatePage,
      Set<Long> notAvailableRowIds, long firstRowId) {
    this.name = name;
    this.colPageDict = colPageDict;
    this.firstRowId = firstRowId;

    values = new IndexRemovingCompressedLongArray(delegatePage.getValues(), //
        notAvailableRowIds.stream().map(l -> (int) (l - delegatePage.getFirstRowId()))
            .collect(DiqubeCollectors.toNavigableSet()),
        0L);
  }

  @Override
  public LongDictionary<?> getColumnPageDict() {
    return colPageDict;
  }

  @Override
  public CompressedLongArray<?> getValues() {
    return values;
  }

  @Override
  public long getFirstRowId() {
    return firstRowId;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header this
        colPageDict.calculateApproximateSizeInBytes() + //
        values.calculateApproximateSizeInBytes() + //
        name.length() + //
        8;
  }

  @Override
  public void setFirstRowId(long firstRowId) {
    this.firstRowId = firstRowId;
  }

  @Override
  public void setName(String name) {
    this.name = name;
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
