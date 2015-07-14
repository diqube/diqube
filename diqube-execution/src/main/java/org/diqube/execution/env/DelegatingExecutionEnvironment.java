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
package org.diqube.execution.env;

import java.util.Map;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;

/**
 * An {@link ExecutionEnvironment} that delegates to another {@link ExecutionEnvironment} object and overriding specific
 * columns.
 *
 * @author Bastian Gloeckle
 */
public class DelegatingExecutionEnvironment extends AbstractExecutionEnvironment
    implements VersionedExecutionEnvironment {

  private ExecutionEnvironment delegate;
  private int version;
  private boolean isModifiable = true;

  public DelegatingExecutionEnvironment(ExecutionEnvironment delegate, int version) {
    this.delegate = delegate;
    this.version = version;
  }

  @Override
  public TableShard getTableShardIfAvailable() {
    return delegate.getTableShardIfAvailable();
  }

  @Override
  public long getFirstRowIdInShard() {
    return delegate.getFirstRowIdInShard();
  }

  @Override
  protected LongColumnShard delegateGetLongColumnShard(String name) {
    return delegate.getLongColumnShard(name);
  }

  @Override
  protected StringColumnShard delegateGetStringColumnShard(String name) {
    return delegate.getStringColumnShard(name);
  }

  @Override
  protected DoubleColumnShard delegateGetDoubleColumnShard(String name) {
    return delegate.getDoubleColumnShard(name);
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  protected Map<String, ColumnShard> delegateGetAllColumnShards() {
    return delegate.getAllColumnShards();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[version=" + getVersion() + "]";
  }

  /**
   * Make this {@link ExecutionEnvironment} not accept any calls to the store* methods in the future anymore,
   * effectively making it unmodifiable.
   */
  public void makeUnmodifiable() {
    isModifiable = false;
  }

  @Override
  public void storeTemporaryLongColumnShard(LongColumnShard column) {
    if (!isModifiable)
      throw new UnsupportedOperationException("The intermediary ExecutionEnvironment is unmodifiable.");
    super.storeTemporaryLongColumnShard(column);
  }

  @Override
  public void storeTemporaryStringColumnShard(StringColumnShard column) {
    if (!isModifiable)
      throw new UnsupportedOperationException("The intermediary ExecutionEnvironment is unmodifiable.");
    super.storeTemporaryStringColumnShard(column);
  }

  @Override
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column) {
    if (!isModifiable)
      throw new UnsupportedOperationException("The intermediary ExecutionEnvironment is unmodifiable.");
    super.storeTemporaryDoubleColumnShard(column);
  }
}
