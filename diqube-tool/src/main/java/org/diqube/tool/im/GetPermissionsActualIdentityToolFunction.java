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
package org.diqube.tool.im;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.thrift.TException;
import org.diqube.remote.query.thrift.IdentityService.Iface;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gets permissions of a specific user.
 *
 * @author Bastian Gloeckle
 */
@IsActualIdentityToolFunction(identityFunctionName = GetPermissionsActualIdentityToolFunction.FUNCTION_NAME,
    shortDescription = GetPermissionsActualIdentityToolFunction.DESCRIPTION)
public class GetPermissionsActualIdentityToolFunction extends AbstractActualIdentityToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(GetPermissionsActualIdentityToolFunction.class);

  public static final String FUNCTION_NAME = "permissions";
  public static final String DESCRIPTION = "Retrieves the permissions of a specific user." + "\n\nNeeds parameters:\n" //
      + "* User (-" + IdentityToolFunction.OPT_PARAM_USER + ")";
  private String paramUser;

  @Override
  public void initializeOptionalParams(String paramUser, String paramPassword, String paramPermission,
      String paramPermissionObject, String paramEmail) {
    if (paramUser == null)
      throw new RuntimeException("Parameters missing! See help of this function.");

    this.paramUser = paramUser;
  }

  @Override
  public void doExecute(Ticket ticket, Iface identityService)
      throws AuthenticationException, AuthorizationException, TException {

    Map<String, List<String>> permissions = identityService.getPermissions(ticket, paramUser);

    System.out.println("Permissions of user '" + paramUser + "':");
    System.out.println();
    List<String> permissionNames = permissions.keySet().stream().sorted().collect(Collectors.toList());
    for (String perm : permissionNames) {
      List<String> objects = permissions.get(perm);
      if (objects != null && !objects.isEmpty()) {
        System.out.println(perm + ":");
        objects.sort(Comparator.naturalOrder());
        for (String object : objects) {
          System.out.println("\t" + object);
        }
        System.out.println();
      } else {
        System.out.println(perm);
      }
    }

    if (permissionNames.isEmpty())
      System.out.println("**none**");
  }

}
