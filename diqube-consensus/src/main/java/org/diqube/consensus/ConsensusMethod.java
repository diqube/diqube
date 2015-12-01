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
package org.diqube.consensus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.atomix.copycat.client.Operation;

/**
 * Denotes a method that should be callable through the consensus cluster, i.e. the method will be called on all cluster
 * nodes reliably by first distributing the information of the {@link Operation} in the cluster and then, when the
 * operation is committed, the method will be called.
 * 
 * <p>
 * Use this annotation only in an interface, which itself has the {@link ConsensusStateMachine} annotation.
 *
 * @author Bastian Gloeckle
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
public @interface ConsensusMethod {
  /**
   * @return The {@link Operation} class which is used on-the-wire to communicate a call to this method.
   */
  Class<? extends Operation<?>> dataClass();
}
