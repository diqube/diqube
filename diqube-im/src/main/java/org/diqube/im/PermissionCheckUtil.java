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
package org.diqube.im;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.thrift.v1.SPermission;
import org.diqube.im.thrift.v1.SUser;
import org.diqube.remote.query.thrift.Ticket;

/**
 * Utility class to check permissions of users.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class PermissionCheckUtil {

  private static final String NONE = "none";

  @Config(ConfigKey.SUPERUSER)
  private String superuser;

  @Inject
  private SUserProvider userProvider;

  private boolean superuserEnabled;

  @PostConstruct
  public void initialize() {
    superuserEnabled = superuser != null && !superuser.equals("") && !superuser.equals(NONE);
  }

  public boolean hasPermission(Ticket ticket, String permission) {
    if (isSuperuser(ticket))
      return true;

    SUser user = userProvider.getUser(ticket.getClaim().getUsername());
    if (user == null)
      return false;

    for (SPermission perm : user.getPermissions()) {
      if (perm.getPermissionName().equals(permission)) {
        return !perm.isSetObjects() || perm.getObjects().isEmpty();
      }
    }
    return false;
  }

  public boolean hasPermission(Ticket ticket, String permission, String object) {
    if (isSuperuser(ticket))
      return true;

    SUser user = userProvider.getUser(ticket.getClaim().getUsername());
    if (user == null)
      return false;

    for (SPermission perm : user.getPermissions()) {
      if (perm.getPermissionName().equals(permission)) {
        if (!perm.isSetObjects())
          return false;
        return perm.getObjects().contains(object);
      }
    }
    return false;
  }

  public boolean isSuperuser(Ticket ticket) {
    if (!superuserEnabled)
      return false;

    return ticket.getClaim().isIsSuperUser() && ticket.getClaim().getUsername().equals(superuser);
  }

  public boolean isSuperuser(String username) {
    if (!superuserEnabled)
      return false;

    return username.equals(superuser);
  }
}
