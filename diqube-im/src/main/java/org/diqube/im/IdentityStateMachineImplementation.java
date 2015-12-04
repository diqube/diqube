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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.im.thrift.v1.SUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.server.Commit;

/**
 * Implementation of {@link IdentityStateMachine} that is executed locally on all cluster nodes.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachineImplementation
public class IdentityStateMachineImplementation implements IdentityStateMachine {
  private static final Logger logger = LoggerFactory.getLogger(IdentityStateMachineImplementation.class);

  private static final String FILE_PREFIX = "identity-";
  private static final String FILE_SUFFIX = ".bak";

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;

  private ConcurrentMap<String, Commit<?>> previous = new ConcurrentHashMap<>();
  private ConcurrentMap<String, SUser> users = new ConcurrentHashMap<>();

  private File internalDbDirFile;

  @PostConstruct
  public void initialize() {
    internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);
  }

  @Override
  public void setUser(Commit<SetUser> commit) {
    Commit<?> prev = previous.put(commit.operation().getUser().getUsername(), commit);

    users.put(commit.operation().getUser().getUsername(), commit.operation().getUser());
    storeCurrentUsers(commit.index());

    if (prev != null)
      prev.clean();
  }

  @Override
  public SUser getUser(Commit<GetUser> commit) {
    String userName = commit.operation().getUserName();
    commit.close();
    return users.get(userName);
  }

  @Override
  public void deleteUser(Commit<DeleteUser> commit) {
    Commit<?> prev = previous.put(commit.operation().getUserName(), commit);
    users.remove(commit.operation().getUserName());
    storeCurrentUsers(commit.index());

    if (prev != null)
      prev.clean();
    commit.clean();
  }

  private void storeCurrentUsers(long newestCommitId) {
    File[] filesToDelete =
        internalDbDirFile.listFiles((dir, name) -> name.startsWith(FILE_PREFIX) && name.endsWith(FILE_SUFFIX));

    File newFile = new File(internalDbDirFile, FILE_PREFIX + String.format("%020d", newestCommitId) + FILE_SUFFIX);

    List<SUser> writeUsers = new ArrayList<>(users.values());
    try {
      IdentityFileWriter writer = new IdentityFileWriter(newFile.getName(), new FileOutputStream(newFile), writeUsers);
      if (writer.write()) {
        for (File f : filesToDelete)
          f.delete();
      }
    } catch (FileNotFoundException e) {
      logger.error("Error while writing identities file", e);
    }
  }

}
