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
package org.diqube.queries;

import java.util.UUID;

import org.diqube.context.AutoInstatiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider for new Query UUIDs and Execution UUIDs.
 *
 * @see QueryUuid
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryUuidProvider {
  private static final Logger logger = LoggerFactory.getLogger(QueryUuidProvider.class);

  public UUID createNewQueryUuid(String diql) {
    UUID res = UUID.randomUUID();
    logger.info("New query UUID {} for query '{}'", res, diql);
    return res;
  }

  public UUID createNewExecutionUuid(UUID queryUuid, String description) {
    UUID res = UUID.randomUUID();
    logger.info("New execution UUID {} for query {}: {}", res, queryUuid, description);
    return res;
  }
}
