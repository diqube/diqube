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

import javax.inject.Inject;

import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.params.RSAKeyParameters;
import org.bouncycastle.crypto.params.RSAPrivateCrtKeyParameters;
import org.bouncycastle.crypto.signers.RSADigestSigner;
import org.diqube.context.AutoInstatiate;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.util.Pair;

/**
 * Service that can sign {@link Ticket}s and verify integrity on those that have been signed already.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TicketSignatureService {

  @Inject
  private TicketRsaKeyManager keyManager;

  /**
   * Checks if a {@link Ticket} has a valid signature.
   * 
   * @param deserializedTicket
   *          The result of {@link TicketUtil#deserialize(ByteBuffer)} of the serialized {@link Ticket}.
   * @return true if {@link Ticket} signature is valid.
   */
  public boolean isValidTicketSignature(Pair<Ticket, byte[]> deserializedTicket) {
    for (RSAKeyParameters pubKey : keyManager.getPublicValidationKeys()) {
      RSADigestSigner signer = new RSADigestSigner(new SHA256Digest());
      signer.init(false, pubKey);
      signer.update(deserializedTicket.getRight(), 0, deserializedTicket.getRight().length);
      if (signer.verifySignature(deserializedTicket.getLeft().getSignature()))
        return true;
    }
    return false;
  }

  /**
   * Calculates the signature of a ticket and updates the given {@link Ticket} object directly.
   * 
   * @throws IllegalStateException
   *           If ticket cannot be signed.
   */
  public void signTicket(Ticket ticket) throws IllegalStateException {
    byte[] serialized = TicketUtil.serialize(ticket);
    byte[] claimBytes = TicketUtil.deserialize(ByteBuffer.wrap(serialized)).getRight();

    RSAPrivateCrtKeyParameters signingKey = keyManager.getPrivateSigningKey();

    if (signingKey == null)
      throw new IllegalStateException("Cannot sign ticket because there is no private signing key available.");

    RSADigestSigner signer = new RSADigestSigner(new SHA256Digest());
    signer.init(true, signingKey);
    signer.update(claimBytes, 0, claimBytes.length);
    try {
      byte[] signature = signer.generateSignature();
      ticket.setSignature(signature);
    } catch (DataLengthException | CryptoException e) {
      throw new IllegalStateException("Cannot sign ticket", e);
    }
  }

}
