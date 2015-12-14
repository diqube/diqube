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
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.thrift.TicketClaim;
import org.diqube.util.Pair;

/**
 * Checks the overall validity of {@link Ticket}s.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TicketValidityService {

  /**
   * Milliseconds duration of timeouts the cleanup method should not remove Tickets that would be invalid because of
   * their claims (valid until)
   */
  public static final long CLEANUP_NO_REMOVE_MOST_RECENT_MS = 10 * 60 * 1_000L; // 10 mins

  /**
   * From {@link TicketClaim#getValidUntil()} to {@link TicketInfo} of tickets that have been marked invalid.
   */
  private NavigableMap<Long, Set<TicketInfo>> invalidTickets = new ConcurrentSkipListMap<>();

  /** Internal strategy on when to try to execute cleanup. Do this randomly in 1-2% of calls by default. */
  private CleanupStrategy cleanupStrategy = () -> ThreadLocalRandom.current().nextInt(128) < 2;
  private ReentrantLock cleanupLock = new ReentrantLock();

  /** Provides the current timestamp. Extracted for tests. */
  private TimestampProvider timestampProvider = () -> System.currentTimeMillis();

  @Inject
  private TicketSignatureService ticketSignatureService;

  /**
   * Checks if a ticket is valid. For performance reasons, {@link #isTicketValid(ByteBuffer)} should be used instead of
   * this one.
   * 
   * @return true if {@link Ticket} signature is valid.
   */
  public boolean isTicketValid(Ticket ticket) {
    return isTicketValid(ByteBuffer.wrap(TicketUtil.serialize(ticket)));
  }

  /**
   * Checks if a {@link Ticket} is valid. For performance reasons, {@link #isTicketValid(ByteBuffer)} should be used
   * instead of this one.
   * 
   * @throws AuthenticationException
   *           is thrown if the ticket is invalid.
   */
  public void validateTicket(Ticket ticket) throws AuthenticationException {
    if (!isTicketValid(ticket))
      throw new AuthenticationException("Ticket for user '" + ticket.getClaim().getUsername() + "' is invalid!");
  }

  /**
   * Checks if a serialized {@link Ticket} is valid.
   * 
   * @return true if {@link Ticket} is valid.
   */
  public boolean isTicketValid(ByteBuffer serializedTicket) {
    Pair<Ticket, byte[]> p = TicketUtil.deserialize(serializedTicket);

    return isTicketValid(p);
  }

  /**
   * Checks if a ticket that was deserialized by {@link TicketUtil#deserialize(ByteBuffer)} is valid.
   * 
   * @param deserializedTicket
   *          Result of {@link TicketUtil#deserialize(ByteBuffer)}
   * @return true if Ticket is valid.
   */
  public boolean isTicketValid(Pair<Ticket, byte[]> deserializedTicket) {
    return isTicketValid(deserializedTicket, false, false);
  }

  /**
   * Internal method: Check validity with optionally disabling specific checks.
   * 
   * @param deserializedTicket
   *          Result of {@link TicketUtil#deserialize(ByteBuffer)}
   * @param ignoreInvalidatedTickets
   *          true if the list of invalidated tickets (= tickets that logged out) should be ignored.
   * @param ignoreSignature
   *          true if the signature should not be checked.
   * @return <code>true</code> if valid according to the parameters.
   */
  private boolean isTicketValid(Pair<Ticket, byte[]> deserializedTicket, boolean ignoreInvalidatedTickets,
      boolean ignoreSignature) {
    Ticket t = deserializedTicket.getLeft();

    if (timestampProvider.now() > t.getClaim().getValidUntil())
      // "now" is after "valid until"
      return false;

    if (!ignoreInvalidatedTickets) {
      Set<TicketInfo> ticketsInvalid = invalidTickets.get(t.getClaim().getValidUntil());
      if (ticketsInvalid != null) {
        boolean ticketIdInvalid = ticketsInvalid.stream()
            .anyMatch(invalidTicket -> invalidTicket.getTicketId().equals(t.getClaim().getTicketId()));

        if (ticketIdInvalid)
          // Ticket was marked as invalid (e.g. logout)
          return false;
      }
    }

    if (cleanupStrategy.shouldCleanup())
      executeCleanup();

    return ignoreSignature || ticketSignatureService.isValidTicketSignature(deserializedTicket);
  }

  /**
   * Check if given {@link Ticket} is valid, when ignoring the local "invalidated ticket" list (= ignore the list of
   * {@link Ticket}s that have logged out but would otherwise be valid).
   *
   * <p>
   * <b>Only call this method when you're sure you want to ignore logged out tickets!</b> You most likely want to use
   * {@link #isTicketValid(Ticket)}!
   * 
   * @return <code>true</code> if ticket is valid when ignoring locally logged out tickets.
   */
  public boolean isTicketValidIgnoringInvalidatedTickets(Ticket t) {
    return isTicketValid(TicketUtil.deserialize(ByteBuffer.wrap(TicketUtil.serialize(t))), true, false);
  }

  /**
   * Checks if a serialized {@link Ticket} is valid.
   * 
   * @throws AuthenticationException
   *           is thrown if the ticket is invalid.
   */
  public void validateTicket(ByteBuffer serializedTicket) throws AuthenticationException {
    if (!isTicketValid(serializedTicket)) {
      Ticket ticket = TicketUtil.deserialize(serializedTicket).getLeft();
      throw new AuthenticationException("Ticket for user '" + ticket.getClaim().getUsername() + "' is invalid!");
    }
  }

  /**
   * Mark the given ticket as invalid, future calls to the validation methods will return false with the given ticket.
   * 
   * <p>
   * This should be called if a user logged out for example.
   */
  public void markTicketAsInvalid(TicketInfo ticketInfo) {
    Set<TicketInfo> ticketInfosInvalid =
        invalidTickets.computeIfAbsent(ticketInfo.getValidUntil(), k -> new ConcurrentSkipListSet<>());
    ticketInfosInvalid.add(ticketInfo);
  }

  /**
   * @return All {@link TicketInfo}s currently marked as invalid. Note that tickets that were passed to
   *         {@link #markTicketAsInvalid(TicketInfo)} will be cleaned up from time to time, if their "valid until" value
   *         has passed. So this method might not return those Tickets that because invalid because of the values of
   *         their claim anyway.
   */
  public Set<TicketInfo> getInvalidTicketInfos() {
    Set<TicketInfo> res = invalidTickets.values().stream().flatMap(s -> s.stream()).collect(Collectors.toSet());
    return res;
  }

  private void executeCleanup() {
    if (cleanupLock.tryLock()) {
      try {
        invalidTickets.headMap(timestampProvider.now() - CLEANUP_NO_REMOVE_MOST_RECENT_MS).clear();
      } finally {
        cleanupLock.unlock();
      }
    }
  }

  // for tests
  /* package */ void setTimestampProvider(TimestampProvider timestampProvider) {
    this.timestampProvider = timestampProvider;
  }

  /* package */ static interface CleanupStrategy {
    boolean shouldCleanup();
  }

  /* package */ static interface TimestampProvider {
    long now();
  }
}
