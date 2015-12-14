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
package org.diqube.ticket;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.thrift.base.util.RUuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link IdentityCallbackService}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class IdentityCallbackHandler implements IdentityCallbackService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(IdentityCallbackHandler.class);

  @Inject
  private TicketValidityService validityService;

  @Override
  public void ticketBecameInvalid(TicketInfo ticketInfo) throws TException {
    validityService.markTicketAsInvalid(ticketInfo);
    logger.info("Marked ticket {}, valid until {} as invalid.", RUuidUtil.toUuid(ticketInfo.getTicketId()),
        ticketInfo.getValidUntil());
  }

}
