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
package org.diqube.loader.columnshard;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.colshard.ColumnPageFactory;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.loader.LoaderColumnInfo;

/**
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ColumnShardBuilderFactory {

  @Inject
  private ColumnShardFactory columnShardFactory;

  @Inject
  private ColumnPageFactory columnPageFactory;

  public ColumnShardBuilderManager createColumnShardBuilderManager(LoaderColumnInfo columnInfo,
      long firstRowIdInShard) {
    return new ColumnShardBuilderManager(columnShardFactory, columnPageFactory, columnInfo, firstRowIdInShard);
  }

  public SparseColumnShardBuilder<Object> createSparseColumnShardBuilder(String colName) {
    return new SparseColumnShardBuilder<Object>(columnShardFactory, columnPageFactory, colName);
  }

}
