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
package org.diqube.plan.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.diqube.plan.optimizer.OptimizerComparisonInfo;
import org.diqube.util.ColumnOrValue;

/**
 * Represents a restriction that can either be present in a WHERE or in a HAVING clause in a select stmt.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 *
 * @author Bastian Gloeckle
 */
public abstract class ComparisonRequest {

  public enum Operator {
    EQ, GT_EQ, GT, LT, LT_EQ
  }

  private OptimizerComparisonInfo optimizerComparisonInfo;

  public abstract <T extends ComparisonRequest> Collection<T> findRecursivelyAllOfType(Class<T> type);

  /**
   * Used for optimizing the {@link ComparisonRequest}
   */
  public OptimizerComparisonInfo getOptimizerComparisonInfo() {
    return optimizerComparisonInfo;
  }

  public void setOptimizerComparisonInfo(OptimizerComparisonInfo optimizerComparisonInfo) {
    this.optimizerComparisonInfo = optimizerComparisonInfo;
  }

  public static class Leaf extends ComparisonRequest {
    private Operator op;
    private String leftColumnName;
    private ColumnOrValue right;

    public String getLeftColumnName() {
      return leftColumnName;
    }

    public void setLeftColumnName(String leftColumnName) {
      this.leftColumnName = leftColumnName;
    }

    public Operator getOp() {
      return op;
    }

    public void setOp(Operator op) {
      this.op = op;
    }

    public ColumnOrValue getRight() {
      return right;
    }

    public void setRight(ColumnOrValue right) {
      this.right = right;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ComparisonRequest> Collection<T> findRecursivelyAllOfType(Class<T> type) {
      Collection<T> res = new ArrayList<T>();
      if (type.equals(Leaf.class))
        res.add((T) this);
      return res;
    }

    @Override
    public String toString() {
      return "[" + getLeftColumnName().toString() + " " + op.toString() + " " + getRight().toString() + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((leftColumnName == null) ? 0 : leftColumnName.hashCode());
      result = prime * result + ((op == null) ? 0 : op.hashCode());
      result = prime * result + ((right == null) ? 0 : right.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof Leaf))
        return false;
      Leaf other = (Leaf) obj;
      if (leftColumnName == null) {
        if (other.leftColumnName != null)
          return false;
      } else if (!leftColumnName.equals(other.leftColumnName))
        return false;
      if (op != other.op)
        return false;
      if (right == null) {
        if (other.right != null)
          return false;
      } else if (!right.equals(other.right))
        return false;
      return true;
    }
  }

  public abstract static class DelegateComparisonRequest extends ComparisonRequest {
    private ComparisonRequest left;
    private ComparisonRequest right;

    public ComparisonRequest getRight() {
      return right;
    }

    public void setRight(ComparisonRequest right) {
      this.right = right;
    }

    public ComparisonRequest getLeft() {
      return left;
    }

    public void setLeft(ComparisonRequest left) {
      this.left = left;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ComparisonRequest> Collection<T> findRecursivelyAllOfType(Class<T> type) {
      Collection<T> res = new ArrayList<T>();

      res.addAll(getLeft().findRecursivelyAllOfType(type));
      res.addAll(getRight().findRecursivelyAllOfType(type));

      if (type.equals(this.getClass()))
        res.add((T) this);
      return res;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((left == null) ? 0 : left.hashCode());
      result = prime * result + ((right == null) ? 0 : right.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof DelegateComparisonRequest))
        return false;
      DelegateComparisonRequest other = (DelegateComparisonRequest) obj;
      if (left == null) {
        if (other.left != null)
          return false;
      } else if (!left.equals(other.left))
        return false;
      if (right == null) {
        if (other.right != null)
          return false;
      } else if (!right.equals(other.right))
        return false;
      return true;
    }
  }

  public static class And extends DelegateComparisonRequest {
    @Override
    public String toString() {
      return "And[" + getLeft().toString() + "," + getRight().toString() + "]";
    }
  }

  public static class Or extends DelegateComparisonRequest {

    @Override
    public String toString() {
      return "Or[" + getLeft().toString() + "," + getRight().toString() + "]";
    }
  }

  public static class Not extends ComparisonRequest {
    private ComparisonRequest child;

    public ComparisonRequest getChild() {
      return child;
    }

    public void setChild(ComparisonRequest child) {
      this.child = child;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ComparisonRequest> Collection<T> findRecursivelyAllOfType(Class<T> type) {
      List<T> res = new ArrayList<>();
      if (type.equals(Not.class))
        res.add((T) this);
      res.addAll(child.findRecursivelyAllOfType(type));
      return res;
    }

    @Override
    public String toString() {
      return "Not[" + child.toString() + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((child == null) ? 0 : child.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof Not))
        return false;
      Not other = (Not) obj;
      if (child == null) {
        if (other.child != null)
          return false;
      } else if (!child.equals(other.child))
        return false;
      return true;
    }

  }

}
