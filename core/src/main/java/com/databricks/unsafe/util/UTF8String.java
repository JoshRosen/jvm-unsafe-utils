package com.databricks.unsafe.util;

import java.io.UnsupportedEncodingException;

/**
 * A String encoded in UTF-8 as long representing the string's length, followed by a
 * contiguous region of bytes; see http://en.wikipedia.org/wiki/UTF-8 for details.
 */
public final class UTF8String {

  private UTF8String() {
    // Make the default constructor private, since this only holds static methods.
    // See UTF8StringPointer for a more object-oriented interface to UTF8String data.
  }

  /**
   * Return the length of the string, in bytes (NOT characters), not including
   * the space to store the length itself.
   */
  static long getLengthInBytes(Object baseObject, long baseOffset) {
    return PlatformDependent.UNSAFE.getLong(baseObject, baseOffset);
  }

  public static String toJavaString(Object baseObject, long baseOffset) {
    final long lengthInBytes = getLengthInBytes(baseObject, baseOffset);
    final byte[] bytes = new byte[(int) lengthInBytes];
    PlatformDependent.UNSAFE.copyMemory(
      baseObject,
      baseOffset + 8,  // skip over the length
      bytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      lengthInBytes
    );
    String str = null;
    try {
      str = new String(bytes, "utf-8");
    } catch (UnsupportedEncodingException e) {
      PlatformDependent.throwException(e);
    }
    return str;
  }

  /**
   * Write a Java string in UTF8String format to the specified memory location.
   *
   * @return the number of bytes written, including the space for tracking the string's length.
   */
  public static long createFromJavaString(Object baseObject, long baseOffset, String str) {
    final byte[] strBytes = str.getBytes();
    final long strLengthInBytes = strBytes.length;
    PlatformDependent.UNSAFE.putLong(baseObject, baseOffset, strLengthInBytes);
    PlatformDependent.copyMemory(
      strBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      baseObject,
      baseOffset + 8,
      strLengthInBytes
    );
    return (8 + strLengthInBytes);
  }

}
