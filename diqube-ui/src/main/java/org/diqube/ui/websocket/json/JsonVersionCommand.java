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
package org.diqube.ui.websocket.json;

import java.io.IOException;

import javax.inject.Inject;

import org.diqube.buildinfo.BuildInfo;
import org.diqube.ui.websocket.json.JsonPayloadSerializer.JsonPayloadSerializerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Command that returns version information when executed.
 *
 * @author Bastian Gloeckle
 */
public class JsonVersionCommand extends JsonCommand {
  private static final Logger logger = LoggerFactory.getLogger(JsonVersionCommand.class);

  public static final String PAYLOAD_TYPE = "version";

  @Inject
  @JsonIgnore
  private JsonPayloadSerializer serializer;

  @Override
  public String getPayloadType() {
    return PAYLOAD_TYPE;
  }

  @Override
  public void execute() throws RuntimeException {
    JsonVersionResultPayload res = new JsonVersionResultPayload();
    res.setBuildTimestamp(BuildInfo.getTimestamp());
    res.setGitCommitShort(BuildInfo.getGitCommitShort());
    res.setGitCommitLong(BuildInfo.getGitCommitLong());
    try {
      String serializedRes = serializer.serialize(res);
      getWebsocketSession().getAsyncRemote().sendText(serializedRes);
      getWebsocketSession().close();
    } catch (JsonPayloadSerializerException e) {
      logger.error("Could not serialize version result", e);
      try {
        getWebsocketSession().close();
      } catch (IOException e1) {
        logger.error("Could not close Websocket", e);
      }
    } catch (IOException e) {
      logger.error("Could not close Websocket", e);
    }
  }

}
