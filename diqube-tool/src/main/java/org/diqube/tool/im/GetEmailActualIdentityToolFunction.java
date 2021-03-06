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
import org.diqube.thrift.base.thrift.Ticket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves the email of a specific user.
 *
 * @author Bastian Gloeckle
 */
@IsActualIdentityToolFunction(identityFunctionName = GetEmailActualIdentityToolFunction.FUNCTION_NAME,
    shortDescription = GetEmailActualIdentityToolFunction.DESCRIPTION)
public class GetEmailActualIdentityToolFunction extends AbstractActualIdentityToolFunction {
  private static final Logger logger = LoggerFactory.getLogger(GetEmailActualIdentityToolFunction.class);

  public static final String FUNCTION_NAME = "getemail";
  public static final String DESCRIPTION = "Retrieves the email of a specific user." + "\n\nNeeds parameters:\n" //
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

    System.out.println(identityService.getEmail(ticket, paramUser));
  }

}
