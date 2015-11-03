package org.diqube.plan.request;

/**
 * The "FROM" clause of a select stmt.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 * 
 * @author Bastian Gloeckle
 */
public class FromRequest {
  private String table;
  private boolean isFlattened;
  private String flattenByField;

  public FromRequest(String table) {
    this.table = table;
    isFlattened = false;
  }

  public FromRequest(String origTable, String flattenByField) {
    table = origTable;
    this.flattenByField = flattenByField;
    isFlattened = true;
  }

  public String getTable() {
    return table;
  }

  public boolean isFlattened() {
    return isFlattened;
  }

  public String getFlattenByField() {
    return flattenByField;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((flattenByField == null) ? 0 : flattenByField.hashCode());
    result = prime * result + (isFlattened ? 1231 : 1237);
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof FromRequest))
      return false;
    FromRequest other = (FromRequest) obj;
    if (flattenByField == null) {
      if (other.flattenByField != null)
        return false;
    } else if (!flattenByField.equals(other.flattenByField))
      return false;
    if (isFlattened != other.isFlattened)
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }
}
