package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryLocation;

public final class Row {

  private static final int WIDTH = 8;

  /*
   * Each tuple has three parts: [null bit set] [values] [variable length portion]
   *
   * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
   * one bit per field.
   *
   * In the `values` region, we store one 8-byte word per field. For fields that hold fixed-length
   * primitive types, such as long, double, or int, we store the value directly in the word. For
   * fields with non-primitive or variable-length values, we store a relative offset (w.r.t. the
   * base address of the row) that points to the beginning of the variable-length field.
   */

  private final Object baseObject;
  private final long baseOffset;
  private final int numFields;
  /** The width of the null tracking bit set, in bytes */
  private final int bitSetWidth;

  public Row(MemoryLocation memory, int numFields) {
    assert numFields >= 0 : "numFields should >= 0";
    this.bitSetWidth = ((numFields / 64) + 1) * 8;
    this.baseObject = memory.getBaseObject();
    this.baseOffset = memory.getBaseOffset();
    this.numFields = numFields;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should <= " + numFields;
  }

  boolean isNullAt(int index) {
    assertIndexIsValid(index);
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    return ((PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + (index / 64)) & mask) != 0);
  }

  void setNull(int index) {
    assertIndexIsValid(index);
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long word = PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + (index / 64));
    PlatformDependent.UNSAFE.putLong(baseObject, baseOffset + (index / 64), word | mask);
  }

  void setNotNull(int index) {
    assertIndexIsValid(index);
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long word = PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + (index / 64));
    PlatformDependent.UNSAFE.putLong(baseObject, baseOffset + (index / 64), word & ~mask);
  }

  long getLong(int index) {
    assertIndexIsValid(index);
    return PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + bitSetWidth + index * WIDTH);
  }

  void putLong(int index, long value) {
    assertIndexIsValid(index);
    PlatformDependent.UNSAFE.putLong(baseObject, baseOffset + bitSetWidth + index * WIDTH, value);
  }

  float getFloat(int index) {
    assertIndexIsValid(index);
    return PlatformDependent.UNSAFE.getFloat(baseObject, baseOffset + bitSetWidth + index * WIDTH);
  }

  void getString(int index, UTF8StringPointer string) {
    assertIndexIsValid(index);
    // This offset is in words and is measured relative to the start of the row
    final long stringDataOffset = getLong(index);
    string.setObjAndOffset(baseObject, baseOffset + stringDataOffset * WIDTH);
  }

  // TODO: all of the other primitive types

  // TODO: string, decimal, date, seq, list, map, struct

  /**
   * @return true if there are any NULL values in this row.
   */
  boolean anyNull() {
    for (int i = 0; i <= bitSetWidth / 8; i++) {
      if (PlatformDependent.UNSAFE.getLong(baseObject, baseOffset + i) != (~0L)) {
        return true;
      }
    }
    return false;
  }

  // TODO: equals, hashCode, and toString()
}
