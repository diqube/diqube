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
package org.diqube.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Set;

/**
 * A {@link ObjectInputStream} that limits reading whitelisted classes.
 * 
 * <p>
 * Will throw a {@link ClassNotFoundException} when reading an object if a not-whitelisted class should be read.
 * 
 * <p>
 * This is to mitigate remote code execution vulnerabilities that came up with
 * http://foxglovesecurity.com/2015/11/06/what-do-weblogic-websphere-jboss-jenkins-opennms-and-your-application-have-in-
 * common-this-vulnerability/ (see also e.g.
 * https://blogs.apache.org/foundation/entry/apache_commons_statement_to_widespread)
 *
 * @author Bastian Gloeckle
 */
public class SafeObjectInputStream extends ObjectInputStream {

  private Set<String> whitelistedClassNames;

  public SafeObjectInputStream(InputStream in, Set<String> whitelistedClassNames) throws IOException {
    super(in);
    this.whitelistedClassNames = whitelistedClassNames;
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
    if (!whitelistedClassNames.contains(desc.getName()))
      throw new ClassNotFoundException("Class '" + desc.getName() + "' is not whitelisted.");

    return super.resolveClass(desc);
  }

}
