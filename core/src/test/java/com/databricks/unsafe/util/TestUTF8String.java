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
    final long bytesWritten =
      UTF8String.createFromJavaString(memory.getBaseObject(), memory.getBaseOffset(), javaStr);
    Assert.assertEquals(8 + javaStrBytes.length, bytesWritten);
    final UTF8StringPointer utf8String = new UTF8StringPointer();
    utf8String.setObjAndOffset(memory.getBaseObject(), memory.getBaseOffset());
    Assert.assertEquals(javaStr, utf8String.toJavaString());
  }
}
