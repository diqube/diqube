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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Consumer;

import org.diqube.function.aggregate.util.SerializedAVLTreeDigest;

/**
 * Default {@link IntermediateResultSerializationResolver}.
 *
 * @author Bastian Gloeckle
 */
@IntermediateResultSerialization
public class DefaultIntermediateResultSerializationResolver implements IntermediateResultSerializationResolver {

  @Override
  public void resolve(Consumer<Class<? extends Serializable>> enableConsumer) {
    // allow followign classes to be used as values in IntermediateResult
    enableConsumer.accept(BigInteger.class);
    enableConsumer.accept(BigDecimal.class);
    enableConsumer.accept(SerializedAVLTreeDigest.class);
    enableConsumer.accept(Number.class);
  }

}
