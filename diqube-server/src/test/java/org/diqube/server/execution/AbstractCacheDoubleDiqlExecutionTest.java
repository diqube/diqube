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
package org.diqube.server.execution;

import org.diqube.data.ColumnType;
import org.testng.annotations.Factory;

/**
 * All subclasses will execute their test methods in a second round based on {@link CacheDoubleTestUtil}.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractCacheDoubleDiqlExecutionTest<T> extends AbstractDiqlExecutionTest<T> {
  public AbstractCacheDoubleDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Factory
  public Object[] cacheDoubleTests() {
    // execute all tests again. In the second run, execute each test twice on a single bean context.
    try {
      return CacheDoubleTestUtil.createTestObjects(this);
    } catch (Throwable t) {
      throw new RuntimeException("Exception while factorying for class " + this.getClass().getName(), t);
    }
  }
}
