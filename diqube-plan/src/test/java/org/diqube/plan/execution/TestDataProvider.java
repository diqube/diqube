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
package org.diqube.plan.execution;

/**
 * Provides test data of a specific data type, solely based on input long values.
 * 
 * <p>
 * This means that e.g. for a String implementation of this class, the class will transform input long values into
 * strings, which in turn can be fed to test columns etc.
 *
 * @author Bastian Gloeckle
 */
public interface TestDataProvider<T> {
  /**
   * Provide one <b>v</b>alue of the test data type based on the given long. All <, <=, >, >= and = relations on the
   * returned values hold exactly the way that they hold on the long values.
   */
  public T v(long no);

  /**
   * Just like {@link #v(long)}, but the returned value will be formatted in a way so the returned string can be used
   * directly in a diql test query. This is useful for example in a WHERE statement, where the value of a column should
   * be compared to a constant - the constant would be created by this method.
   * 
   * <p>
   * Example: For a String implementation of this interface, the returned value will be the string enclosed in single
   * quotes to enable correct parsing of the diql query.
   */
  public String vDiql(long no);

  /**
   * Just like {@link #v(long)}, but creates a whole <b>a</b>rray of values.
   */
  public T[] a(long... no);

  /**
   * Creates an empty array of the target data type with the given length.
   */
  public T[] emptyArray(int length);
}
