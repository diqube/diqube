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
package org.diqube.server;

import org.diqube.buildinfo.BuildInfo;
import org.diqube.context.Profiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Executable class that starts a diqube server.
 *
 * @author Bastian Gloeckle
 */
public class Server {
  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  private static Server INSTANCE = null;

  public static Server getInstance() {
    return Server.INSTANCE;
  }

  public static void main(String[] args) {
    Server.INSTANCE = new Server();
    Server.INSTANCE.serve();
  }

  public void serve() {
    System.out
        .println("diqube is free software: you can redistribute it and/or modify it under the terms of the GNU Affero "
            + "General Public License as published by the Free Software Foundation, either version 3 of the License, or "
            + "(at your option) any later version. This program is distributed in the hope that it will be useful, but "
            + "WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR "
            + "PURPOSE. See the GNU Affero General Public License for more details.");
    System.out.println(
        "Copyright (C) 2015 Bastian Gloeckle. For source code and more information see http://diqube.org and/or http://github.com/diqube/diqube.");

    logger.info("Starting diqube context and diqube server...");
    logger.info("This executable is based on diqube commit {} and was built on {}.", BuildInfo.getGitCommitLong(),
        BuildInfo.getTimestamp());

    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment().setActiveProfiles(Profiles.ALL);
    ctx.scan("org.diqube");
    ctx.refresh();

    ServerImplementation serverImpl = ctx.getBean(ServerImplementation.class);
    serverImpl.serve();
  }
}
