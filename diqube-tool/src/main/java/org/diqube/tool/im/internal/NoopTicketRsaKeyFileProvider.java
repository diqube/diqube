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
package org.diqube.tool.im.internal;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.ticket.TicketRsaKeyFileProvider;
import org.diqube.util.Triple;
import org.springframework.context.annotation.Profile;

/**
 * Noop {@link TicketRsaKeyFileProvider} for diqube-tool, as this does not validate tickets anyhow.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.TOOL)
public class NoopTicketRsaKeyFileProvider implements TicketRsaKeyFileProvider {

  @Override
  public CompletableFuture<List<Triple<String, IOExceptionSupplier<InputStream>, String>>> getPemFiles() {
    return CompletableFuture.completedFuture(new ArrayList<>());
  }

  @Override
  public boolean filesWithPrivateKeyAreRequired() {
    return false;
  }

}
