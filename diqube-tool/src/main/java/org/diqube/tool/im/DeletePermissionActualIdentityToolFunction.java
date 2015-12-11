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

import org.apache.thrift.TException;
import org.diqube.remote.query.thrift.IdentityService.Iface;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.remote.query.thrift.OptionalString;
import org.diqube.remote.query.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes a permission of a specific user.
 *
 * @author Bastian Gloeckle
 */
@IsActualIdentityToolFunction(identityFunctionName = DeletePermissionActualIdentityToolFunction.FUNCTION_NAME,
    shortDescription = DeletePermissionActualIdentityToolFunction.DESCRIPTION)
public class DeletePermissionActualIdentityToolFunction extends AbstractActualIdentityToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(DeletePermissionActualIdentityToolFunction.class);

  public static final String FUNCTION_NAME = "deletepermission";
  public static final String DESCRIPTION = "Removes a permission of a specific user." + "\n\nNeeds parameters:\n" //
      + "* User (-" + IdentityToolFunction.OPT_PARAM_USER + ")\n" //
      + "* Permission (-" + IdentityToolFunction.OPT_PARAM_PERMISSION + ")" //
      + "* optional Permission object (-" + IdentityToolFunction.OPT_PARAM_PERMISSION_OBJECT + ")";
  private String paramUser;
  private String paramPermission;
  private String paramPermissionObject;

  @Override
  public void initializeOptionalParams(String paramUser, String paramPassword, String paramPermission,
      String paramPermissionObject, String paramEmail) {
    if (paramUser == null || paramPermission == null)
      throw new RuntimeException("Parameters missing! See help of this function.");

    this.paramUser = paramUser;
    this.paramPermission = paramPermission;
    this.paramPermissionObject = paramPermissionObject;
  }

  @Override
  public void doExecute(Ticket ticket, Iface identityService)
      throws AuthenticationException, AuthorizationException, TException {

    if (paramPermissionObject == null)
      logger.info("Deleting permission '{}' of user '{}'", paramPermission, paramUser);
    else
      logger.info("Deleting permission '{}' on '{}' of user '{}'", paramPermission, paramPermissionObject, paramUser);

    OptionalString object = new OptionalString();
    if (paramPermissionObject != null)
      object.setValue(paramPermissionObject);

    identityService.removePermission(ticket, paramUser, paramPermission, object);
  }

}
