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

import org.diqube.data.types.str.StringColumnShard;
import org.diqube.data.types.str.dict.StringDictionary;
import org.diqube.queries.QueryRegistry;

/**
 * A facade for {@link StringColumnShard} that provides some resolve-methods and gathers statistics.
 *
 * @author Bastian Gloeckle
 */
public class QueryableStringColumnShardFacade extends AbstractQueryableColumnShardFacade
    implements QueryableStringColumnShard {

  public QueryableStringColumnShardFacade(StringColumnShard delegate, boolean isTempColumn,
      QueryRegistry queryRegistry) {
    super(delegate, isTempColumn, queryRegistry);
  }

  /**
   * Creates a {@link QueryableStringColumnShardFacade} that does not collect statistics.
   */
  public QueryableStringColumnShardFacade(StringColumnShard delegate) {
    super(delegate, false, null);
  }

  @Override
  public StringColumnShard getDelegate() {
    return (StringColumnShard) super.getDelegate();
  }

  @Override
  public StringDictionary<?> getColumnShardDictionary() {
    return (StringDictionary<?>) super.getColumnShardDictionary();
  }
}
