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

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.thrift.base.thrift.Ticket;

/**
 * Utility class to check is a user is a valid superuser.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class SuperuserCheckUtil {

  private static final String NONE = "none";

  @Config(ConfigKey.SUPERUSER)
  private String superuser;

  private boolean superuserEnabled;

  @PostConstruct
  public void initialize() {
    superuserEnabled = superuser != null && !superuser.equals("") && !superuser.equals(NONE);
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
