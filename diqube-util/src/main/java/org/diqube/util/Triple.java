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

/**
 * An arbitrary triple of values.
 *
 * @author Bastian Gloeckle
 */
public class Triple<L, M, R> {
  final private L left;

  final private M middle;

  final private R right;

  public Triple(L left, M middle, R right) {
    this.left = left;
    this.middle = middle;
    this.right = right;
  }

  public L getLeft() {
    return left;
  }

  public M getMiddle() {
    return middle;
  }

  public R getRight() {
    return right;
  }

  @Override
  public int hashCode() {
    return ((left != null) ? left.hashCode() : 7) ^ ((right != null) ? right.hashCode() : 11)
        ^ ((middle != null) ? middle.hashCode() : 17);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Triple)) {
      return false;
    }

    @SuppressWarnings("rawtypes")
    Triple p = (Triple) obj;
    if (left != null && !left.equals(p.left) || (left == null && p.left != null))
      return false;
    if (middle != null && !middle.equals(p.middle) || (middle == null && p.middle != null))
      return false;
    if (right != null && !right.equals(p.right) || (right == null && p.right != null))
      return false;

    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getSimpleName());
    sb.append("[left=");
    sb.append((left != null) ? left.toString() : "null");
    sb.append(", middle=");
    sb.append((middle != null) ? middle.toString() : "null");
    sb.append(", right=");
    sb.append((right != null) ? right.toString() : "null");
    sb.append(")");
    return sb.toString();
  }

}
