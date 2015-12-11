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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketClaim;
import org.diqube.thrift.util.RememberingTransport;
import org.diqube.util.Pair;

/**
 * Untility to de-/serialize {@link Ticket}s.
 *
 * @author Bastian Gloeckle
 */
public class TicketUtil {
  /**
   * Serialize the given ticket into a byte array.
   * 
   * @throws IllegalArgumentException
   *           if something goes wrong.
   */
  public static byte[] serialize(Ticket t) throws IllegalArgumentException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (TTransport transport = new TIOStreamTransport(baos)) {
      TCompactProtocol protocol = new TCompactProtocol(transport);
      t.write(protocol);
      transport.flush();
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serailize ticket", e);
    }

    return baos.toByteArray();
  }

  /**
   * Deserialize bytes into a {@link Ticket} and capture the bytes of the serialized stream that identify the
   * {@link TicketClaim}.
   * 
   * @return {@link Pair} of deserialized {@link Ticket} and the bytes that were used in the serialized form to describe
   *         the {@link TicketClaim} (that what is signed in the {@link Ticket}).
   */
  public static Pair<Ticket, byte[]> deserialize(ByteBuffer serializedTicket) {
    byte[] data = new byte[serializedTicket.remaining()];
    serializedTicket.get(data);
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    try (TTransport origTransport = new TIOStreamTransport(bais)) {
      RememberingTransport rememberingTransport = new RememberingTransport(origTransport);
      TCompactProtocol compactProtocol = new TCompactProtocol(rememberingTransport);
      PartialRememberingProtocol rememberingProtocol =
          new PartialRememberingProtocol(compactProtocol, rememberingTransport, //
              // The first "struct" that is read is the "claim" struct.
              // THIS DEPENDS ON THE THRIFT DEFINITION!
              0);

      Ticket t = new Ticket();
      t.read(rememberingProtocol);
      byte[] claimBytes = rememberingProtocol.getRememberedBytes();

      return new Pair<>(t, claimBytes);
    } catch (TException e) {
      throw new IllegalArgumentException("Could not deserialize ticket", e);
    }
  }

  /**
   * A Protocol that remembers the bytes of a specific child "struct" of a thrift message. It is meant to be used with
   * {@link Ticket}.
   * 
   * <p>
   * The message looks something like the following:
   * 
   * <p>
   * [MessageBegin][Ticket struct begin][TicketClaim struct begin][ticket claim info][TicketClaim struct end]...
   */
  private static class PartialRememberingProtocol extends TProtocolDecorator {
    private RememberingTransport rememberingTransport;

    private int structLevel = 0;
    private int topLevelStruct = 0;

    private int topLevelStructToBeRecorded;

    private byte[] rememberedBytes;

    public PartialRememberingProtocol(TProtocol protocol, RememberingTransport rememberingTransport,
        int topLevelStructToBeRecorded) {
      super(protocol);
      this.rememberingTransport = rememberingTransport;
      this.topLevelStructToBeRecorded = topLevelStructToBeRecorded;
    }

    @Override
    public TStruct readStructBegin() throws TException {
      if (structLevel == 1) {
        if (topLevelStructToBeRecorded == topLevelStruct)
          rememberingTransport.startRemeberingReadBytes();
      }
      structLevel++;
      return super.readStructBegin();
    }

    @Override
    public void readStructEnd() throws TException {
      super.readStructEnd();

      structLevel--;
      if (structLevel == 1) {
        if (topLevelStructToBeRecorded == topLevelStruct)
          rememberedBytes = rememberingTransport.stopRememberingReadBytes();

        topLevelStruct++;
      }
    }

    public byte[] getRememberedBytes() {
      return rememberedBytes;
    }

  }
}
