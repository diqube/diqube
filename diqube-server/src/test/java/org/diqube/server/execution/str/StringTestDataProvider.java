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
package org.diqube.server.execution.str;

import java.util.stream.LongStream;

import org.diqube.server.execution.TestDataProvider;

/**
 * {@link TestDataProvider} for String.
 *
 * @author Bastian Gloeckle
 */
public class StringTestDataProvider implements TestDataProvider<String> {

  @Override
  public String v(long no) {
    return ((no < 0) ? '0' : '1') + String.format("%017d", Math.abs(no));
  }

  @Override
  public String[] a(long... no) {
    return LongStream.of(no).mapToObj(n -> v(n)).toArray(l -> new String[l]);
  }

  @Override
  public String[] emptyArray(int length) {
    return new String[length];
  }

  @Override
  public String vDiql(long no) {
    return "'" + v(no) + "'";
  }

}
