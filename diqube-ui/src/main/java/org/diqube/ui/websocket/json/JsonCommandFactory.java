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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.Session;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author Bastian Gloeckle
 */
public class JsonCommandFactory {
  private static Map<String, Class<? extends JsonCommand>> commandClasses;

  private JsonFactory jsonFactory = new JsonFactory();

  public JsonCommand createCommand(String clientCommandJson, Session websocketSession) {
    try {
      JsonParser parser = jsonFactory.createParser(clientCommandJson);

      if (!parser.nextToken().equals(JsonToken.START_OBJECT))
        return null;
      if (!parser.nextToken().equals(JsonToken.FIELD_NAME))
        return null;
      String fieldName = parser.getCurrentName();
      if (!"cmd".equals(fieldName))
        return null;
      if (!parser.nextToken().equals(JsonToken.VALUE_STRING))
        return null;
      String command = parser.getValueAsString();
      if (!parser.nextToken().equals(JsonToken.FIELD_NAME))
        return null;
      fieldName = parser.getCurrentName();
      if (!"param".equals(fieldName))
        return null;
      if (!parser.nextToken().equals(JsonToken.START_OBJECT))
        return null;
      JsonLocation paramObjectLocation = parser.getCurrentLocation();
      int lastBraces = clientCommandJson.lastIndexOf('}');
      if (lastBraces == -1)
        return null;
      String paramValue = clientCommandJson.substring((int) paramObjectLocation.getCharOffset() - 1, lastBraces);

      if (!commandClasses.containsKey(command))
        return null;

      ObjectMapper mapper = new ObjectMapper();
      JsonCommand cmd = mapper.readValue(paramValue, commandClasses.get(command));
      cmd.setWebsocketSession(websocketSession);

      return cmd;
    } catch (IOException e) {
      return null;
    }
  }

  static {
    commandClasses = new ConcurrentHashMap<>();
    commandClasses.put("query", JsonQueryCommand.class);
  }
}
