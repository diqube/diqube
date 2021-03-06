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
package org.diqube.itest.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.diqube.itest.AbstractDiqubeIntegrationTest;

/**
 * Annotate a test method if that test method needs one or multiple diqube-servers.
 * 
 * The server(s) are available through {@link AbstractDiqubeIntegrationTest#serverControl} and
 * {@link AbstractDiqubeIntegrationTest#clusterControl}.
 *
 * @author Bastian Gloeckle
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NeedsServer {
  /** number of servers needed */
  int servers() default 1;

  /**
   * set to true to manually startup the servers using {@link AbstractDiqubeIntegrationTest#serverControl} and
   * {@link AbstractDiqubeIntegrationTest#clusterControl}.
   */
  boolean manualStart() default false;
}
