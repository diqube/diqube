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

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.im.SUserProvider;
import org.diqube.im.thrift.v1.SPermission;
import org.diqube.im.thrift.v1.SUser;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;

/**
 * Utility class for checking {@link Permissions}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class PermissionsUtil {
  @Inject
  private SUserProvider userProvider;

  /**
   * Checks if a user has a "object-free" permission, i.e. a permission that is not restricted to specific objects.
   * 
   * @param ticket
   *          The ticket of the user to check access for. Must have been validated using a {@link TicketValidityService}
   *          before!
   */
  public boolean hasPermission(Ticket ticket, String permission) {
    if (ticket.getClaim().isIsSuperUser())
      return true;

    SUser user = userProvider.getUser(ticket.getClaim().getUsername());
    if (user == null || !user.isSetPermissions())
      return false;
    for (SPermission perm : user.getPermissions()) {
      if (perm.equals(permission)) {
        // if permission is restricted to objects, user does not have "object-free" permission.
        if (perm.isSetObjects() && !perm.getObjects().isEmpty())
          return false;
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if a user has a "object" permission, i.e. a permission that is restricted to specific objects and contains
   * the given object.
   * 
   * @param ticket
   *          The ticket of the user to check access for. Must have been validated using a {@link TicketValidityService}
   *          before!
   */
  public boolean hasPermission(Ticket ticket, String permission, String object) {
    if (ticket.getClaim().isIsSuperUser())
      return true;

    SUser user = userProvider.getUser(ticket.getClaim().getUsername());
    if (user == null || !user.isSetPermissions())
      return false;
    for (SPermission perm : user.getPermissions()) {
      if (perm.equals(permission)) {
        // if permission is not restricted to objects, user does not have "object" permission.
        if (!perm.isSetObjects() || perm.getObjects().isEmpty())
          return false;

        return perm.getObjects().contains(object);
      }
    }
    return false;
  }
}
