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

import java.util.Collection;
import java.util.Collections;

import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;

/**
 * A flattened {@link Table}, which is based on a delegate normal {@link Table} but was flattened on a specific
 * (repeated) column.
 * 
 * @author Bastian Gloeckle
 */
public class FlattenedTable implements Table {

  private String name;
  private Collection<TableShard> shards;

  /* package */ FlattenedTable(String name, Collection<TableShard> shards) {
    this.name = name;
    this.shards = shards;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<TableShard> getShards() {
    return Collections.unmodifiableCollection(shards);
  }

}
