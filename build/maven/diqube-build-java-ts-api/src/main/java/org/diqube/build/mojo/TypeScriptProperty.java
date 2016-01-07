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

/**
 * Enforces creation of a specific java field to a typescript field when generating the .d.ts file from java classes.
 * 
 * <p>
 * Normal annotated properties will be added to a typescript interface with their corresponding data type.
 * 
 * <p>
 * If the annotated field is "public static final", in addition to the typescript interface, a typescript class will be
 * created (name ends with "Constants") which will hold the values of the public static final fields. This is only
 * allowed for fields which map directly to a typescript native data type (string, number, boolean).
 *
 * @author Bastian Gloeckle
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TypeScriptProperty {
  /**
   * <code>true</code> if the property should be marked as "optional" in .d.ts. This value is ignored for
   * "public static final" fields.
   */
  boolean optional() default false;
}
