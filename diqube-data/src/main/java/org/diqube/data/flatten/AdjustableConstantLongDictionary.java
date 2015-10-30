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
package org.diqube.data.flatten;

import org.apache.thrift.TBase;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * Interface for a constant long dictionary that allows changing the constant value - this is needed when flattenning.
 *
 * @author Bastian Gloeckle
 */
public interface AdjustableConstantLongDictionary<T extends TBase<?, ?>> extends LongDictionary<T> {
  /**
   * Adjust the constant value. Must only be called before this dictionary is used!
   */
  public void setValue(long value);
}
