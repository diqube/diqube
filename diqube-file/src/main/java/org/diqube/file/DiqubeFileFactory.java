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
package org.diqube.file;

import java.io.IOException;
import java.io.OutputStream;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.util.BigByteBuffer;

/**
 * Factory for classes of diqube-file.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeFileFactory {
  @Inject
  private DataSerializationManager dataSerializationManager;

  public DiqubeFileWriter createDiqubeFileWriter(OutputStream outputStream) throws IOException {
    return new DiqubeFileWriter(dataSerializationManager.createSerializer(), outputStream);
  }

  public DiqubeFileReader createDiqubeFileReader(BigByteBuffer diqubeFileData) throws IOException {
    return new DiqubeFileReader(dataSerializationManager.createDeserializer(), diqubeFileData);
  }
}
