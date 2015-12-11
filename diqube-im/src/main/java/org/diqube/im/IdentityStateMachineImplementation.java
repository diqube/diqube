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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.consensus.ConsensusStateMachineImplementation;
import org.diqube.context.InjectOptional;
import org.diqube.file.internaldb.InternalDbFileReader;
import org.diqube.file.internaldb.InternalDbFileReader.ReadException;
import org.diqube.file.internaldb.InternalDbFileWriter;
import org.diqube.file.internaldb.InternalDbFileWriter.WriteException;
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

  private static final String INTERNALDB_FILE_PREFIX = "identity-";
  private static final String INTERNALDB_DATA_TYPE = "identities_v1";

  @Config(DerivedConfigKey.FINAL_INTERNAL_DB_DIR)
  private String internalDbDir;

  private ConcurrentMap<String, Commit<?>> previous = new ConcurrentHashMap<>();
  private ConcurrentMap<String, SUser> users = new ConcurrentHashMap<>();

  @InjectOptional
  private List<UserChangedListener> userChangedListeners;

  private InternalDbFileWriter<SUser> internalDbFileWriter;

  @PostConstruct
  public void initialize() {
    File internalDbDirFile = new File(internalDbDir);
    if (!internalDbDirFile.exists())
      if (!internalDbDirFile.mkdirs())
        throw new RuntimeException("Could not create directory " + internalDbDir);

    try {
      InternalDbFileReader<SUser> internalDbFileReader = new InternalDbFileReader<>(INTERNALDB_DATA_TYPE,
          INTERNALDB_FILE_PREFIX, internalDbDirFile, () -> new SUser());
      List<SUser> users = internalDbFileReader.readNewest();
      if (users != null)
        for (SUser user : users) {
          this.users.put(user.getUsername(), user);
        }
      else
        logger.info("No internaldb for identities available");
    } catch (ReadException e) {
      throw new RuntimeException("Could not load identities file", e);
    }

    internalDbFileWriter = new InternalDbFileWriter<>(INTERNALDB_DATA_TYPE, INTERNALDB_FILE_PREFIX, internalDbDirFile);
  }

  @Override
  public void setUser(Commit<SetUser> commit) {
    Commit<?> prev = previous.put(commit.operation().getUser().getUsername(), commit);

    String username = commit.operation().getUser().getUsername();
    logger.info("Adjusting user '{}'...", username);
    users.put(username, commit.operation().getUser());
    storeCurrentUsers(commit.index());

    if (prev != null)
      prev.clean();

    if (userChangedListeners != null)
      userChangedListeners.forEach(l -> l.userChanged(username));
  }

  @Override
  public SUser getUser(Commit<GetUser> commit) {
    String userName = commit.operation().getUserName();
    commit.close();
    return users.get(userName);
  }

  @Override
  public void deleteUser(Commit<DeleteUser> commit) {
    Commit<?> prev = previous.remove(commit.operation().getUserName());
    String username = commit.operation().getUserName();
    logger.info("Deleting user '{}'...", username);
    users.remove(username);
    storeCurrentUsers(commit.index());

    if (prev != null)
      prev.clean();
    commit.clean();

    if (userChangedListeners != null)
      userChangedListeners.forEach(l -> l.userChanged(username));
  }

  private void storeCurrentUsers(long newestCommitId) {
    try {
      internalDbFileWriter.write(newestCommitId, new ArrayList<>(users.values()));
    } catch (WriteException e) {
      logger.error("Could not write identites internaldb file!", e);
      // this is an error, but we try to continue anyway. When the file is missing, the node might not be able to
      // recover correctly, but for now we can keep working. The admin might want to copy a internaldb file from a
      // different node.
    }
  }

}
