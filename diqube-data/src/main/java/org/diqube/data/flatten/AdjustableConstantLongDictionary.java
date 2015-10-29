package org.diqube.data.flatten;

import org.apache.thrift.TBase;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * Interface for a constant long dictionary that allows changing the constant value - this is needed when flattenning.
 *
 * @author Bastian Gloeckle
 */
public interface AdjustableConstantLongDictionary<T extends TBase<?, ?>> extends LongDictionary<T> {
  /**
   * Adjust the constant value. Must only be called before this dictionary is used!
   */
  public void setValue(long value);
}
