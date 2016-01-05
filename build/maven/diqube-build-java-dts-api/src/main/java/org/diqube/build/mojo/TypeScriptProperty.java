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
package org.diqube.build.mojo;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Map;

/**
 * Enforces creation of a specific java field to a typescript field when generating the .d.ts files from java classes.
 *
 * @author Bastian Gloeckle
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TypeScriptProperty {
  /**
   * If the annotated fields type is a {@link Collection}, use this property to define the type of the elements inside
   * the collection.
   */
  Class<?> collectionType() default Object.class;

  /**
   * If the annotated fields type is a {@link Map}, use this property to define the type of the keys of the map.
   */
  Class<?> mapKeyType() default Object.class;

  /**
   * If the annotated fields type is a {@link Map}, use this property to define the type of the values of the map.
   */
  Class<?> mapValueType() default Object.class;

  /**
   * <code>true</code> if the property should be marked as "optional" in .d.ts.
   */
  boolean optional() default false;
}
