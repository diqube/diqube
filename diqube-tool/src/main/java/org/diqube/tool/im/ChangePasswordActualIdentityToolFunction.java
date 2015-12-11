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
import org.diqube.remote.base.thrift.AuthenticationException;
import org.diqube.remote.base.thrift.AuthorizationException;
import org.diqube.remote.query.thrift.IdentityService.Iface;
import org.diqube.remote.query.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Change the password of a user.
 *
 * @author Bastian Gloeckle
 */
@IsActualIdentityToolFunction(identityFunctionName = ChangePasswordActualIdentityToolFunction.FUNCTION_NAME,
    shortDescription = ChangePasswordActualIdentityToolFunction.DESCRIPTION)
public class ChangePasswordActualIdentityToolFunction extends AbstractActualIdentityToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(ChangePasswordActualIdentityToolFunction.class);

  public static final String FUNCTION_NAME = "changepassword";
  public static final String DESCRIPTION = "Changes the password of a user." + "\n\nNeeds parameters:\n" //
      + "* User (-" + IdentityToolFunction.OPT_PARAM_USER + ")\n" //
      + "* new password (-" + IdentityToolFunction.OPT_PARAM_PASSWORD + ")";
  private String paramUser;

  private String paramPassword;

  @Override
  public void initializeOptionalParams(String paramUser, String paramPassword, String paramPermission,
      String paramPermissionObject, String paramEmail) {
    if (paramUser == null || paramPassword == null)
      throw new RuntimeException("Parameters missing! See help of this function.");

    this.paramUser = paramUser;
    this.paramPassword = paramPassword;
  }

  @Override
  public void doExecute(Ticket ticket, Iface identityService)
      throws AuthenticationException, AuthorizationException, TException {

    logger.info("Changing password of user '{}'", paramUser);
    identityService.changePassword(ticket, paramUser, paramPassword);
  }

}
