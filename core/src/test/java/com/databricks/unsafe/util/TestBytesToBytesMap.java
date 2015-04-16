package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class TestBytesToBytesMap {

  private static final Random rand = new Random(42);

  private static long allocateRandomByteArray(int lengthInWords) {
    assert(lengthInWords > 0);
    final int lengthInBytes = lengthInWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    final long memoryAddr = PlatformDependent.UNSAFE.allocateMemory(lengthInBytes);
    PlatformDependent.copyMemory(
      bytes, PlatformDependent.BYTE_ARRAY_OFFSET, null, memoryAddr, lengthInBytes);
    return memoryAddr;
  }

  @Test
  public void emptyMap() {
    BytesToBytesMap map = new BytesToBytesMap(MemoryAllocator.UNSAFE, 64);
    Assert.assertEquals(0, map.size());
    final int keyLengthInWords = 10;
    final int keyLengthInBytes = keyLengthInWords * 8;
    final long keyPointer = allocateRandomByteArray(keyLengthInWords);
    try {
      Assert.assertFalse(map.lookup(null, keyPointer, keyLengthInBytes).isDefined());
    } finally {
      PlatformDependent.UNSAFE.freeMemory(keyPointer);
      map.free();
    }
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(MemoryAllocator.UNSAFE, 64);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final long keyPointer = allocateRandomByteArray(recordLengthWords);
    final long valuePointer = allocateRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc = map.lookup(null, keyPointer, recordLengthBytes);
      assert(!loc.isDefined());
      loc.storeKeyAndValue(
        null,
        keyPointer,
        recordLengthBytes,
        null,
        valuePointer,
        recordLengthBytes
      );
      assert(map.lookup(null, keyPointer, recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());
    } finally {
      PlatformDependent.UNSAFE.freeMemory(keyPointer);
      PlatformDependent.UNSAFE.freeMemory(valuePointer);
      map.free();
    }
  }
}
