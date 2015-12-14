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
package org.diqube.permission;

/**
 * Permissions in diqube.
 * 
 * <p>
 * A user managed in diqube-im can have multiple permissions. Each permission can optionally have "objects" attached, in
 * which case the permission is valid only for the given objects. Note that each permission is either object-based or
 * not. See the JavaDoc of each permission.
 *
 * @author Bastian Gloeckle
 */
public class Permissions {
  /**
   * Permission to access specific tables.
   * 
   * <p>
   * This permission needs "objects", which in turn are the table names the user has access to.
   */
  public static final String TABLE_ACCESS = "table_access";
}
