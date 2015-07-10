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

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;
import org.diqube.execution.env.ExecutionEnvironment;

/**
 * An {@link OverwritingConsumer} that is called as soon as a specific column is built - for each column it is at most
 * called once. As soon as the method is called, the corresponding column is available in the default
 * {@link ExecutionEnvironment}.
 * 
 * <p>
 * Please note that this consumer will call {@link #columnBuilt(String)} only once!
 * 
 * <p>
 * Compare to {@link ColumnVersionBuiltConsumer}.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(ColumnBuiltConsumer.class)
public interface ColumnBuiltConsumer extends OverwritingConsumer {
  /**
   * @param colName
   *          The column that has been built and is already available through the default {@link ExecutionEnvironment}.
   *          This method will be called only once!
   */
  public void columnBuilt(String colName);
}
