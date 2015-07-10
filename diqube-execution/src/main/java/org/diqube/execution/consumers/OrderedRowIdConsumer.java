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
package org.diqube.execution.consumers;

import java.util.List;

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;
import org.diqube.execution.steps.OrderStep;

/**
 * This {@link OverwritingConsumer} consumes a set of ordered Row IDs.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(OrderedRowIdConsumer.class)
public interface OrderedRowIdConsumer extends OverwritingConsumer {
  /**
   * Will be called as soon as a ordered set of row IDs is available.
   * 
   * <p>
   * If the query being executed orders its results, the only way to get the ordered Row IDs is to set a
   * {@link OrderedRowIdConsumer} to the {@link OrderStep}. It could be that the number of RowIDs provided in this
   * method is smaller than the number of row IDs for which values have been provided by a {@link ColumnValueConsumer}/
   * {@link ColumnDictIdConsumer} - this is the case if the ORDER clause had a LIMIT clause. In that case, only the row
   * IDs that are reported to this method are valid.
   */
  public void consumeOrderedRowIds(List<Long> rowIds);
}
