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
package org.diqube.connection.integrity;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.diqube.connection.integrity.IntegrityCheckingProtocol.IntegrityViolatedException;
import org.diqube.thriftutil.RememberingTransport;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class IntegrityCheckingProtocolTest {
  private static final byte[][] MAC_KEYS = new byte[][] { new byte[] { 0x01, 0x02, 0x03 } };
  private IntegrityCheckingProtocol outputIntegrityValidatingProtocol;
  private TMemoryBuffer outputMemoryBuf;
  private TMemoryInputTransport inputMemoryTrans;
  private IntegrityCheckingProtocol inputIntegrityValidatingProtocol;

  @BeforeMethod
  public void before() {
    outputMemoryBuf = new TMemoryBuffer(0);
    TBinaryProtocol outputBinaryProtocol = new TBinaryProtocol(new RememberingTransport(outputMemoryBuf));
    outputIntegrityValidatingProtocol = new IntegrityCheckingProtocol(outputBinaryProtocol, MAC_KEYS);

    inputMemoryTrans = new TMemoryInputTransport();
    TBinaryProtocol inputBinaryProtocol = new TBinaryProtocol(new RememberingTransport(inputMemoryTrans));
    inputIntegrityValidatingProtocol = new IntegrityCheckingProtocol(inputBinaryProtocol, MAC_KEYS);
  }

  @AfterMethod
  public void after() {

  }

  @Test
  public void sendAndReceive() throws TException {
    // GIVEN
    TMessage msg = new TMessage("a", (byte) 0, 0);
    int content = 100;

    outputIntegrityValidatingProtocol.writeMessageBegin(msg);
    outputIntegrityValidatingProtocol.writeI32(content);
    outputIntegrityValidatingProtocol.writeMessageEnd();

    inputMemoryTrans.reset(outputMemoryBuf.getArray(), 0, outputMemoryBuf.length());

    // WHEN
    TMessage readMsg = inputIntegrityValidatingProtocol.readMessageBegin();
    int readContent = inputIntegrityValidatingProtocol.readI32();
    inputIntegrityValidatingProtocol.readMessageEnd();

    // THEN
    Assert.assertEquals(readMsg, msg, "Expected to read correct TMessage");
    Assert.assertEquals(content, readContent, "Expected to read correct content");
    // and: Expected not to have a validity exception!
  }

  @Test(expectedExceptions = IntegrityViolatedException.class)
  public void sendAndReceiveTamperedMessage() throws TException {
    // GIVEN
    TMessage msg = new TMessage("a", (byte) 0, 0);
    int content = 100;

    outputIntegrityValidatingProtocol.writeMessageBegin(msg);
    outputIntegrityValidatingProtocol.writeI32(content);
    outputIntegrityValidatingProtocol.writeMessageEnd();

    byte[] wireData = outputMemoryBuf.getArray();
    // tamper with the message on the wire
    wireData[wireData.length / 2] = (byte) -wireData[wireData.length / 2];
    inputMemoryTrans.reset(wireData);

    // WHEN
    inputIntegrityValidatingProtocol.readMessageBegin();
    inputIntegrityValidatingProtocol.readI32();
    inputIntegrityValidatingProtocol.readMessageEnd();

    // THEN
    // and: Expected to have a validity exception!
  }

  @Test
  public void noSideeffects() throws TException {
    // GIVEN
    TMessage msg = new TMessage("a", (byte) 0, 0);
    int content = 100;

    outputIntegrityValidatingProtocol.writeMessageBegin(msg);
    outputIntegrityValidatingProtocol.writeI32(content);
    outputIntegrityValidatingProtocol.writeMessageEnd();

    int intermediaryPos = outputMemoryBuf.length();

    // WHEN
    outputIntegrityValidatingProtocol.writeMessageBegin(msg);
    outputIntegrityValidatingProtocol.writeI32(content);
    outputIntegrityValidatingProtocol.writeMessageEnd();

    // THEN
    byte[] firstMsg = new byte[intermediaryPos];
    byte[] secondMsg = new byte[outputMemoryBuf.length() - intermediaryPos];
    System.arraycopy(outputMemoryBuf.getArray(), 0, firstMsg, 0, intermediaryPos);
    System.arraycopy(outputMemoryBuf.getArray(), intermediaryPos, secondMsg, 0, secondMsg.length);

    Assert.assertEquals(firstMsg, secondMsg, "Expected that first and second message are encoded in the same way!");
  }

}
