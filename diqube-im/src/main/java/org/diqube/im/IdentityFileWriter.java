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

import java.io.OutputStream;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.im.thrift.v1.SIdentities;
import org.diqube.im.thrift.v1.SIdentitiesHeader;
import org.diqube.im.thrift.v1.SUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes a simple identities file, containing information about known users and their properties.
 *
 * @author Bastian Gloeckle
 */
public class IdentityFileWriter {
  private static final Logger logger = LoggerFactory.getLogger(IdentityFileWriter.class);
  private static final int VERSION = 1;

  private OutputStream outStream;
  private List<SUser> users;
  private String fileName;

  public IdentityFileWriter(String fileName, OutputStream outStream, List<SUser> users) {
    this.fileName = fileName;
    this.outStream = outStream;
    this.users = users;
  }

  public boolean write() {
    logger.info("Writing updated identities to '{}'...", fileName);
    try (TIOStreamTransport transport = new TIOStreamTransport(outStream)) {
      TCompactProtocol protocol = new TCompactProtocol(transport);

      SIdentitiesHeader header = new SIdentitiesHeader(VERSION);
      header.write(protocol);

      SIdentities identities = new SIdentities(users);
      identities.write(protocol);

      logger.info("Updated identities written to '{}'.", fileName);
      // TIOStreamTransport closes the outStream.
      return true;
    } catch (TException e) {
      logger.error("Could not write identities file '{}'", fileName, e);
      return false;
    }
  }
}
