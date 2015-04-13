package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryLocation;

import java.io.UnsupportedEncodingException;

/**
 * A String encoded in UTF-8 as a contiguous region of bytes, which can be used for
 * comparison, search, see http://en.wikipedia.org/wiki/UTF-8 for details.
 */
public class UTF8String {

  private Object baseObject = null;
  private long baseOffset = 0;

  /** The length of this string, in bytes (NOT characters) */
  private long lengthInBytes = 0;

  UTF8String() {
    // This constructor is intentionally left blank.
  }

  UTF8String(MemoryLocation memoryLocation, long lengthInBytes) {
    this.setBaseObjectAndOffset(memoryLocation.getBaseObject(), memoryLocation.getBaseOffset());
    this.setLengthInBytes(lengthInBytes);
  }

  public void setBaseObjectAndOffset(Object baseObject, long baseOffset) {
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
  }

  public void setLengthInBytes(long newLengthInBytes) {
    assert lengthInBytes >= 0 : "Size should be >= 0";
    this.lengthInBytes = newLengthInBytes;
  }

  @Override
  public String toString() {
    final byte[] bytes = new byte[(int) lengthInBytes];
    PlatformDependent.UNSAFE.copyMemory(
      baseObject,
      baseOffset,
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

}
