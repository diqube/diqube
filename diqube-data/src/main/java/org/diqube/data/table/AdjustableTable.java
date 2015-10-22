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
package org.diqube.data.table;

/**
 * A {@link Table} that can be adjusted, i.e. {@link TableShard}s can be added and removed.
 *
 * @author Bastian Gloeckle
 */
public interface AdjustableTable extends Table {
  /**
   * Adds a {@link TableShard} to this table.
   * 
   * @throws TableShardsOverlappingException
   *           If the rows served by the new tableShard overlap with a tableShard already in the table. If the exception
   *           is thrown, the new TableShard is not added to the table.
   */
  public void addTableShard(TableShard tableShard) throws TableShardsOverlappingException;

  /**
   * Remove the given tableShard from this table.
   * 
   * @return true if the tableShard was contained in this table.
   */
  public boolean removeTableShard(TableShard tableShard);

  public static class TableShardsOverlappingException extends Exception {
    private static final long serialVersionUID = 1L;

    public TableShardsOverlappingException(String msg) {
      super(msg);
    }
  }
}
