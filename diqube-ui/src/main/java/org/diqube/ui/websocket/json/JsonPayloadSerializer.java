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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import org.diqube.context.AutoInstatiate;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serializes arbitrary {@link JsonPayload} objects to JSON.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonPayloadSerializer {
  private ObjectMapper mapper = new ObjectMapper();

  /**
   * Serialize the given JsonPayload.
   * 
   * @throws JsonPayloadSerializerException
   *           If anything goes wrong.
   */
  public String serialize(JsonPayload payload) throws JsonPayloadSerializerException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter osw = new OutputStreamWriter(baos, Charset.forName("UTF-8"));
    String objectJson;
    try {
      mapper.writerFor(payload.getClass()).writeValue(osw, payload);
      osw.close();

      objectJson = new String(baos.toByteArray(), Charset.forName("UTF-8"));
    } catch (IOException e) {
      throw new JsonPayloadSerializerException("Could not serialize result");
    }

    StringBuilder sb = new StringBuilder();
    sb.append("{\"type\": \"");
    sb.append(payload.getPayloadType());
    sb.append("\", \"data\":");
    sb.append(objectJson);
    sb.append("}");

    return sb.toString();
  }

  public static class JsonPayloadSerializerException extends Exception {
    private static final long serialVersionUID = 1L;

    public JsonPayloadSerializerException(String msg) {
      super(msg);
    }
  }
}
