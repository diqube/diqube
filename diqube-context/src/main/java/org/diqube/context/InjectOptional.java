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
package org.diqube.context;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Similar to {@link Inject}, but marks the injection as optional - if no bean is available to be wired, the field will
 * simply be <code>null</code>.
 * 
 * This is meaningful to be used when wiring all available listeners (from diqube-listeners) for example.
 *
 * @author Bastian Gloeckle
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Autowired(required = false)
public @interface InjectOptional {

}
