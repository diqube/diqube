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

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.cache.ConstantTimeCache;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.im.IdentityStateMachine.GetUser;
import org.diqube.im.thrift.v1.SUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SUserProvider} that caches {@link SUser} objects for a configured amount of time, so we do not need to query
 * the consensus cluster too often.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class CachingConsensusSUserProvider implements SUserProvider, IdentityStateMachine.UserChangedListener {
  private static final Logger logger = LoggerFactory.getLogger(CachingConsensusSUserProvider.class);

  private static final Long K2 = 0L;
  private static final SUser NO_USER = new SUser();

  @Config(ConfigKey.USER_INFORMATION_CACHE_SEC)
  private int cacheSec;

  @Inject
  private ConsensusClient consensusClient;

  private ConstantTimeCache<String, Long, SUser> userCache;

  @PostConstruct
  public void initialize() {
    userCache = new ConstantTimeCache<>(cacheSec * 1_000L);
  }

  @Override
  public SUser getUser(String username) {
    SUser res = userCache.get(username, K2);
    if (res != null)
      return (res != NO_USER) ? res : null;

    return loadUserFromConsensus(username);
  }

  private SUser loadUserFromConsensus(String username) {
    SUser newUser;
    try (ClosableProvider<IdentityStateMachine> p = consensusClient.getStateMachineClient(IdentityStateMachine.class)) {
      newUser = p.getClient().getUser(GetUser.local(username));
    } catch (ConsensusClusterUnavailableException e) {
      logger.warn("Could not access consensus cluster and cannot load user info therefore!", e);
      return null;
    }

    userCache.offer(username, K2, (newUser != null) ? newUser : NO_USER);
    return newUser;
  }

  @Override
  public void userChanged(String username) {
    // not reliable, but do best-effort deletion of user info from the cache if info is changed.
    userCache.delete(username, K2);
  }

}
