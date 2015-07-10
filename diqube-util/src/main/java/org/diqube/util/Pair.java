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
 * An arbitrary pair of values.
 *
 * @author Bastian Gloeckle
 */
public class Pair<L, R> {
  final private L left;

  final private R right;

  public Pair(Pair<L, R> other) {
    this.left = other.left;
    this.right = other.right;
  }

  public Pair(L left, R right) {
    this.left = left;
    this.right = right;
  }

  public L getLeft() {
    return left;
  }

  public R getRight() {
    return right;
  }

  @Override
  public int hashCode() {
    return ((left != null) ? left.hashCode() : 7) ^ ((right != null) ? right.hashCode() : 11);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Pair)) {
      return false;
    }

    @SuppressWarnings("rawtypes")
    Pair p = (Pair) obj;
    if (left != null && !left.equals(p.left) || (left == null && p.left != null))
      return false;
    if (right != null && !right.equals(p.right) || (right == null && p.right != null))
      return false;

    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Pair(left=");
    sb.append((left != null) ? left.toString() : "null");
    sb.append(", right=");
    sb.append((right != null) ? right.toString() : "null");
    sb.append(")");
    return sb.toString();
  }

}
