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
package org.diqube.function.aggregate.result.serialization;

import java.io.Serializable;
import java.util.function.Consumer;

import org.diqube.function.AggregationFunction;
import org.diqube.function.IntermediaryResult;

/**
 * Capable of resolving whitelisted classes for serialization.
 */
public interface IntermediateResultSerializationResolver {
  /**
   * Called when a {@link IntermediateResultSerialization} annotation is on the class.
   * 
   * @param enableConsumer
   *          call this consumer with the class that should be whitelisted for java serialization (= class that is used
   *          in {@link IntermediaryResult} of {@link AggregationFunction}s and implements {@link Serializable}).
   */
  public void resolve(Consumer<Class<? extends Serializable>> enableConsumer);
}
