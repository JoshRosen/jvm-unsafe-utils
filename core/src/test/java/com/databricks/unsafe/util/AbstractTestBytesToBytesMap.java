package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;
import com.databricks.unsafe.util.memory.MemoryBlock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public abstract class AbstractTestBytesToBytesMap {

  protected final Random rand = new Random(42);

  protected final MemoryAllocator allocator = getMemoryAllocator();

  protected abstract MemoryAllocator getMemoryAllocator();

  protected MemoryBlock allocateRandomByteArray(int lengthInWords) {
    assert(lengthInWords > 0);
    final int lengthInBytes = lengthInWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    final MemoryBlock memory = allocator.allocate(lengthInBytes);
    PlatformDependent.copyMemory(
      bytes,
      PlatformDependent.BYTE_ARRAY_OFFSET,
      memory.getBaseObject(),
      memory.getBaseOffset(),
      lengthInBytes);
    return memory;
  }

  @Test
  public void emptyMap() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    Assert.assertEquals(0, map.size());
    final int keyLengthInWords = 10;
    final int keyLengthInBytes = keyLengthInWords * 8;
    final MemoryBlock key = allocateRandomByteArray(keyLengthInWords);
    try {
      Assert.assertFalse(
        map.lookup(key.getBaseObject(), key.getBaseOffset(), keyLengthInBytes).isDefined());
    } finally {
      allocator.free(key);
    }
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final MemoryBlock key = allocateRandomByteArray(recordLengthWords);
    final MemoryBlock value = allocateRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc = map.lookup(
        key.getBaseObject(), key.getBaseOffset(), recordLengthBytes);
      assert(!loc.isDefined());
      loc.storeKeyAndValue(
        key.getBaseObject(),
        key.getBaseOffset(),
        recordLengthBytes,
        value.getBaseObject(),
        value.getBaseOffset(),
        recordLengthBytes
      );
      assert(map.lookup(key.getBaseObject(), key.getBaseOffset(), recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());

      final byte[] expectedKeyData = new byte[recordLengthBytes];
      PlatformDependent.UNSAFE.copyMemory(
        key.getBaseObject(),
        key.getBaseOffset(),
        expectedKeyData,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      final byte[] actualKeyData = new byte[recordLengthBytes];
      PlatformDependent.UNSAFE.copyMemory(
        loc.getKeyAddress().getBaseObject(),
        loc.getKeyAddress().getBaseOffset(),
        actualKeyData,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      Assert.assertArrayEquals(expectedKeyData, actualKeyData);

      final byte[] expectedValueData = new byte[recordLengthBytes];
      PlatformDependent.UNSAFE.copyMemory(
        value.getBaseObject(),
        value.getBaseOffset(),
        expectedValueData,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      final byte[] actualValueData = new byte[recordLengthBytes];
      PlatformDependent.UNSAFE.copyMemory(
        loc.getValueAddress().getBaseObject(),
        loc.getValueAddress().getBaseOffset(),
        actualValueData,
        PlatformDependent.BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      Assert.assertArrayEquals(expectedValueData, actualValueData);

      try {
        loc.storeKeyAndValue(
          key.getBaseObject(),
          key.getBaseOffset(),
          recordLengthBytes,
          value.getBaseObject(),
          value.getBaseOffset(),
          recordLengthBytes
        );
        Assert.fail("Should not be able to set a new value for a key");
      } catch (IllegalStateException e) {
        // Expected exception; do nothing.
      }
    } finally {
      allocator.free(key);
      allocator.free(value);
      map.free();
    }
  }

}
