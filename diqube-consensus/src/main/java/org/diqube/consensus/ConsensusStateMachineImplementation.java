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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.diqube.context.AutoInstatiate;

/**
 * Interface which denotes the implementation of a specific {@link ConsensusStateMachine} annotated interface.
 * 
 * <p>
 * The class annotated with this annotation will be used on each diqube-server when an operation has been distributed in
 * the cluster successfully and then the corresponding method will be called.
 *
 * <p>
 * Note that all classes annotated with this annotation are {@link AutoInstatiate}.
 *
 * @author Bastian Gloeckle
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@AutoInstatiate
public @interface ConsensusStateMachineImplementation {

}
