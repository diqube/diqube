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

import java.nio.ByteBuffer;

import org.diqube.remote.query.thrift.Ticket;
import org.diqube.remote.query.thrift.TicketClaim;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class TicketUtilTest {
  @Test
  public void deserializeDoesNotGiveSignatureBytes() {
    // GIVEN
    // two tickets with the same claim, different sig
    Ticket t = new Ticket();
    t.setClaim(new TicketClaim());
    t.getClaim().setUsername("abc");
    t.getClaim().setValidUntil(123L);
    t.getClaim().setIsSuperUser(true);
    t.setSignature(new byte[] { 5, 6, 7 });

    Ticket t2 = new Ticket(t);
    t2.setSignature(new byte[] { 10, 11, 12, 13 });

    // WHEN
    // serialize and deserialize both
    byte[] serialized = TicketUtil.serialize(t);
    byte[] serialized2 = TicketUtil.serialize(t2);

    Pair<Ticket, byte[]> deserialized = TicketUtil.deserialize(ByteBuffer.wrap(serialized));
    Pair<Ticket, byte[]> deserialized2 = TicketUtil.deserialize(ByteBuffer.wrap(serialized2));

    // THEN
    Assert.assertEquals(deserialized.getLeft().getClaim(), deserialized2.getLeft().getClaim(),
        "Deserialized ticket claims should be equal");
    Assert.assertEquals(deserialized.getRight(), deserialized2.getRight(), "Returned 'claim bytes' should be equal");
    Assert.assertEquals(deserialized.getLeft(), t, "First deserialized ticket should be equal to original one");
    Assert.assertEquals(deserialized2.getLeft(), t2, "Second deserialized ticket should be equal to original one");
  }

  @Test
  public void deserializeDoesGiveClaimBytes() {
    // GIVEN
    // two different
    Ticket t = new Ticket();
    t.setClaim(new TicketClaim());
    t.getClaim().setUsername("abc");
    t.getClaim().setValidUntil(123L);
    t.getClaim().setIsSuperUser(false);
    t.setSignature(new byte[] { 5, 6, 7 });

    Ticket t2 = new Ticket();
    t2.setClaim(new TicketClaim());
    t2.getClaim().setUsername("def");
    t2.getClaim().setValidUntil(789L);
    t2.getClaim().setIsSuperUser(false);
    t2.setSignature(new byte[] { 5, 6, 7 });

    // WHEN
    // serialize and deserialize both
    byte[] serialized = TicketUtil.serialize(t);
    byte[] serialized2 = TicketUtil.serialize(t2);

    Pair<Ticket, byte[]> deserialized = TicketUtil.deserialize(ByteBuffer.wrap(serialized));
    Pair<Ticket, byte[]> deserialized2 = TicketUtil.deserialize(ByteBuffer.wrap(serialized2));

    // THEN
    Assert.assertNotEquals(deserialized.getLeft().getClaim(), deserialized2.getLeft().getClaim(),
        "Deserialized ticket claims should NOT be equal");
    Assert.assertNotEquals(deserialized.getRight(), deserialized2.getRight(),
        "Returned 'claim bytes' should NOT be equal");
    Assert.assertEquals(deserialized.getLeft(), t, "First deserialized ticket should be equal to original one");
    Assert.assertEquals(deserialized2.getLeft(), t2, "Second deserialized ticket should be equal to original one");
  }
}
