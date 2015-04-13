package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryLocation;

/**
 * A pointer to UTF8String data.
 */
public class UTF8StringPointer extends MemoryLocation {

  public long getLengthInBytes() { return UTF8String.getLengthInBytes(obj, offset); }

  public String toJavaString() { return UTF8String.toJavaString(obj, offset); }

  @Override public String toString() { return toJavaString(); }
}
