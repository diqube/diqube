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

import java.io.Serializable;

/**
 * An arbitrary pair of values.
 * 
 * <p>
 * A {@link Pair} is {@link Comparable} when both objects are {@link Comparable}. The left side will be compared first
 * and only if the left is equal, the right will be compared.
 * 
 * <p>
 * A {@link Pair} is {@link Serializable} if its contents are {@link Serializable}.
 *
 * @author Bastian Gloeckle
 */
public class Pair<L, R> implements Comparable<Pair<L, R>>, Serializable {
  private static final long serialVersionUID = 1L;

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

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Pair<L, R> o) {
    int compareResLeft = ((Comparable<L>) left).compareTo(o.getLeft());
    if (compareResLeft == 0) {
      return ((Comparable<R>) right).compareTo(o.getRight());
    } else
      return compareResLeft;
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
