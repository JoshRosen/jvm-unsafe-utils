package com.databricks.unsafe.util;

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  /**
   * Optimized byte array equality check for 8-byte-word-aligned byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean wordAlignedArrayEquals(
      Object leftBaseObject,
      long leftBaseOffset,
      Object rightBaseObject,
      long rightBaseOffset,
      long arrayLengthInBytes) {
    for (int i = 0; i < arrayLengthInBytes; i += 8) {
      final long left =
        PlatformDependent.UNSAFE.getLong(leftBaseObject, leftBaseOffset + i);
      final long right =
        PlatformDependent.UNSAFE.getLong(rightBaseObject, rightBaseOffset + i);
      if (left != right) return false;
    }
    return true;
  }
}
