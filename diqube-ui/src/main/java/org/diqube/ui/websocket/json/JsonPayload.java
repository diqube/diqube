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

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * JSON data that is transferred between the browser and the UI server on websockets is deserialized into
 * {@link JsonPayload} objects.
 * 
 * Implementing classes may contain fields annotated with both, {@link Inject} and {@link JsonIgnore} in which case the
 * {@link JsonPayloadDeserializer} will wire beans from the bean context accordingly. This is probably most interesting
 * for classes implementing the sub-interface {@link JsonCommand}.
 * 
 * @author Bastian Gloeckle
 */
public interface JsonPayload {
  /**
   * @return unique string identifying the type of payload. This string will be used in JavaScript, too, to identify
   *         different types of payloads.
   */
  public String getPayloadType();
}
