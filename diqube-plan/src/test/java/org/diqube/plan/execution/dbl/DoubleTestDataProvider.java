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
package org.diqube.plan.execution.dbl;

import java.util.stream.LongStream;

import org.diqube.plan.execution.TestDataProvider;

/**
 * {@link TestDataProvider} for Double.
 *
 * @author Bastian Gloeckle
 */
public class DoubleTestDataProvider implements TestDataProvider<Double> {

  @Override
  public Double v(long no) {
    return no + (.1 * (no % 10));
  }

  @Override
  public Double[] a(long... no) {
    return LongStream.of(no).mapToObj(n -> v(n)).toArray(l -> new Double[l]);
  }

  @Override
  public Double[] emptyArray(int length) {
    return new Double[length];
  }

  @Override
  public String vDiql(long no) {
    return v(no).toString();
  }

}
