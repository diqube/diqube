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
package org.diqube.consensus.test;

import org.diqube.consensus.ConsensusIsLeaderProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.springframework.context.annotation.Profile;

/**
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.TEST_CONSENSUS)
public class TestConsensusIsLeaderProvider implements ConsensusIsLeaderProvider {

  @Override
  public boolean isLeader() {
    return true;
  }

}
