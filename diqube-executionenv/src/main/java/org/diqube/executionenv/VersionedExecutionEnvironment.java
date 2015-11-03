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
package org.diqube.executionenv;

/**
 * A {@link ExecutionEnvironment} that is versioned. The version number is strictly increasing over time and the data
 * that is provided by {@link VersionedExecutionEnvironment} only increases over time. This means that it might either
 * be that the following version of a {@link VersionedExecutionEnvironment} contains values of more columns, a column
 * contains more rows or the values in a specific column were adjusted to the most up-to-date value.
 * 
 * <p>
 * This is only used on the query master while executing a query. The {@link ExecutablePlanStep}s on the query master
 * are wired by a {@link ColumnVersionBuiltConsumer} which effectively leads to the usage of a
 * {@link VersionedExecutionEnvironment} on which the steps are executed. This leads to the fact that the query master
 * actually handles those cases where the columns actually change during execution.
 * 
 * <p>
 * See also comment on {@link ExecutionEnvironment}.
 *
 * @author Bastian Gloeckle
 */
public interface VersionedExecutionEnvironment extends ExecutionEnvironment {
  public int getVersion();
}
