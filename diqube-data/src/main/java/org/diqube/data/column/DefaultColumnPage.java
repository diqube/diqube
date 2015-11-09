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
package org.diqube.data.column;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;
import org.diqube.data.serialize.thrift.v1.SLongDictionary;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * Default implementation of {@link ColumnPage} which holds values that were loaded from data files e.g.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SColumnPage.class)
public class DefaultColumnPage implements AdjustableColumnPage {
  private LongDictionary<?> columnPageDict;

  private CompressedLongArray<?> values;

  private long firstRowId;

  private String name;

  /** for deserialization */
  public DefaultColumnPage() {

  }

  protected DefaultColumnPage(LongDictionary<?> columnPageDict, CompressedLongArray<?> values, long firstRowId,
      String name) {
    this.columnPageDict = columnPageDict;
    this.values = values;
    this.firstRowId = firstRowId;
    this.name = name;
  }

  @Override
  public LongDictionary<?> getColumnPageDict() {
    return columnPageDict;
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
  public void serialize(DataSerializationHelper mgr, SColumnPage target) throws SerializationException {
    target.setName(name);
    target.setFirstRowId(firstRowId);
    target.setPageDict(mgr.serializeChild(SLongDictionary.class, columnPageDict));
    target.setValues(mgr.serializeChild(SLongCompressedArray.class, values));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SColumnPage source) throws DeserializationException {
    name = source.getName();
    firstRowId = source.getFirstRowId();
    columnPageDict = mgr.deserializeChild(LongDictionary.class, source.getPageDict());
    values = mgr.deserializeChild(CompressedLongArray.class, source.getValues());
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
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this.
        columnPageDict.calculateApproximateSizeInBytes() + //
        values.calculateApproximateSizeInBytes();
  }

}
