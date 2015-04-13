package com.databricks.unsafe.util;

import org.junit.Assert;
import org.junit.Test;

import com.databricks.unsafe.util.memory.MemoryLocation;
import com.databricks.unsafe.util.memory.MemoryBlock;

public class TestUTF8String {

  @Test
  public void toStringTest() {
    final String javaStr = "Hello, World!";
    final byte[] javaStrBytes = javaStr.getBytes();
    final int paddedSizeInWords = javaStrBytes.length / 8 + (javaStrBytes.length % 8 == 0 ? 0 : 1);
    final MemoryLocation memory = MemoryBlock.fromLongArray(new long[paddedSizeInWords]);
    PlatformDependent.copyMemory(
      javaStrBytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      memory.getBaseObject(),
      memory.getBaseOffset(),
      javaStrBytes.length
    );
    final UTF8String utf8String = new UTF8String(memory, javaStrBytes.length);
    Assert.assertEquals(javaStr, utf8String.toString());
  }
}
