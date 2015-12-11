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

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolDecorator;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.diqube.thrift.util.RememberingTransport;
import org.diqube.util.BouncyCastleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TProtocol} which writes HMACs to all messages in order to validate them again when reading a message.
 * 
 * <p>
 * The transport of the protocol that this protocol is based on needs to be a {@link RememberingTransport}!
 * 
 * <p>
 * This protocol will throw a {@link IntegrityViolatedException} when trying to read a message whose integrity is
 * violated.
 *
 * @author Bastian Gloeckle
 */
public class IntegrityCheckingProtocol extends TProtocolDecorator {
  private static final Logger logger = LoggerFactory.getLogger(IntegrityCheckingProtocol.class);

  private static ThreadLocal<Boolean> integrityCheckDisabled = new ThreadLocal<>();

  private RememberingTransport transport;

  private Mac[] mac;

  private TProtocol protocol;

  /**
   * Create the integrity validating protocol.
   * 
   * @param protocol
   *          The protocol this protocol should be based upon.
   * @param macKeys
   *          Secret keys to use for the HMAC algorithm. There needs to be at least 1. The first key will be used to
   *          sign all messages the are written by this protocol. Whn messages are read, a signature of any of the given
   *          keys will count the message as "valid".
   */
  public IntegrityCheckingProtocol(TProtocol protocol, byte[]... macKeys) {
    super(protocol);
    if (!(protocol.getTransport() instanceof RememberingTransport))
      throw new IllegalArgumentException("The transport needs to be a " + RememberingTransport.class.getSimpleName());
    transport = (RememberingTransport) protocol.getTransport();
    this.protocol = protocol;

    if (macKeys.length == 0)
      throw new IllegalArgumentException("Need at least one macKey!");

    try {
      mac = new Mac[macKeys.length];
      for (int i = 0; i < macKeys.length; i++) {
        mac[i] = Mac.getInstance("HmacSHA256", BouncyCastleUtil.getProvider());
        mac[i].init(new SecretKeySpec(macKeys[i], "HmacSHA256"));
      }
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new IllegalStateException("Could not find HMAC algorithm implementation or could not initialize it.", e);
    }
  }

  @Override
  public void writeMessageBegin(TMessage tMessage) throws TException {
    Boolean disabled = integrityCheckDisabled.get();
    if (disabled == null || !disabled)
      transport.startRemeberingWriteBytes();

    super.writeMessageBegin(tMessage);
  }

  @Override
  public void writeMessageEnd() throws TException {
    super.writeMessageEnd();

    Boolean disabled = integrityCheckDisabled.get();
    if (disabled == null || !disabled) {
      byte[] msgData = transport.stopRememberingWriteBytes();
      // logger.trace("Calculating integrity for message {}", msgData);
      byte[] integrityData = mac[0].doFinal(msgData);
      protocol.writeBinary(ByteBuffer.wrap(integrityData));
    }
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    Boolean disabled = integrityCheckDisabled.get();
    if (disabled == null || !disabled)
      transport.startRemeberingReadBytes();

    return super.readMessageBegin();
  }

  @Override
  public void readMessageEnd() throws TException {
    super.readMessageEnd();

    Boolean disabled = integrityCheckDisabled.get();
    if (disabled == null || !disabled) {
      byte[] msgData = transport.stopRememberingReadBytes();
      ByteBuffer integrityBuffer = protocol.readBinary();
      byte[] integrityDataActual = new byte[integrityBuffer.remaining()];
      integrityBuffer.get(integrityDataActual);

      // logger.trace("Validating integrity of message: {}", msgData);
      // logger.trace("Integrity data provided: {}", integrityDataActual);

      for (int i = 0; i < mac.length; i++) {
        byte[] integrityDataExpected = mac[i].doFinal(msgData);
        // logger.trace("Calculated possible valid integrity data: {}", integrityDataExpected);
        if (Arrays.equals(integrityDataActual, integrityDataExpected))
          return;
      }

      logger.error("Received a message with violated integrity!");
      throw new IntegrityViolatedException("Integrity of message violated.");
    }
  }

  /**
   * Message integrity violated.
   */
  public static class IntegrityViolatedException extends TException {

    private static final long serialVersionUID = 1L;

    public IntegrityViolatedException(String msg) {
      super(msg);
    }
  }

  /**
   * {@link TProtocolFactory} for {@link IntegrityCheckingProtocol}.
   */
  public static class Factory implements TProtocolFactory {
    private static final long serialVersionUID = 1L;

    private TProtocolFactory delegateFactory;
    private byte[][] macKeys;

    /**
     * Create the integrity validating protocol.
     * 
     * @param delegateFactory
     *          The factory to be used to create the delegate protocol.
     * @param macKeys
     *          Secret keys to use for the HMAC algorithm. There needs to be at least 1. The first key will be used to
     *          sign all messages the are written by this protocol. Whn messages are read, a signature of any of the
     *          given keys will count the message as "valid".
     */
    public Factory(TProtocolFactory delegateFactory, byte[]... macKeys) {
      this.delegateFactory = delegateFactory;
      this.macKeys = macKeys;
    }

    @Override
    public TProtocol getProtocol(TTransport trans) {
      if (!(trans instanceof RememberingTransport))
        throw new IllegalArgumentException("The transport needs to be a " + RememberingTransport.class.getSimpleName());

      TProtocol delegateProtocol = delegateFactory.getProtocol(trans);

      return new IntegrityCheckingProtocol(delegateProtocol, macKeys);
    }
  }

  /**
   * A {@link TProcessor} that disables integrity checks when reading &writing messages from a
   * {@link IntegrityCheckingProtocol}.
   * 
   * <p>
   * This leads to the MAC not being calculated, written and read from the input.
   */
  public static class IntegrityCheckDisablingProcessor implements TProcessor {
    private TProcessor delegate;

    public IntegrityCheckDisablingProcessor(TProcessor delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      integrityCheckDisabled.set(true);
      try {
        return delegate.process(in, out);
      } finally {
        integrityCheckDisabled.remove();
      }
    }
  }
}
