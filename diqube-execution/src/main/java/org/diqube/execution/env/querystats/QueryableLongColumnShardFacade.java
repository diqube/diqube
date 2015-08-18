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
package org.diqube.execution.env.querystats;

import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.queries.QueryRegistry;

/**
 * A facade for {@link LongColumnShard} that provides some resolve-methods and gathers statistics.
 *
 * @author Bastian Gloeckle
 */
public class QueryableLongColumnShardFacade extends AbstractQueryableColumnShardFacade
    implements QueryableLongColumnShard {

  public QueryableLongColumnShardFacade(LongColumnShard delegate, boolean isTempColumn, QueryRegistry queryRegistry) {
    super(delegate, isTempColumn, queryRegistry);
  }

  @Override
  public LongColumnShard getDelegate() {
    return (LongColumnShard) super.getDelegate();
  }

  @Override
  public LongDictionary<?> getColumnShardDictionary() {
    return (LongDictionary<?>) super.getColumnShardDictionary();
  }
}
