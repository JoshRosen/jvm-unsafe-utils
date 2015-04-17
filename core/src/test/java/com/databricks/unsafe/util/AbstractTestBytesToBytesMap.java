package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;
import com.databricks.unsafe.util.memory.MemoryLocation;
import static com.databricks.unsafe.util.PlatformDependent.BYTE_ARRAY_OFFSET;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

// A few general TODOs:
  // We should re-use the same temporary storage / pointers / etc when copying the key data rather
  // than performing allocations just so that we can do unsafe access to that data.

public abstract class AbstractTestBytesToBytesMap {

  protected final Random rand = new Random(42);

  protected final MemoryAllocator allocator = getMemoryAllocator();

  protected abstract MemoryAllocator getMemoryAllocator();

  protected byte[] getByteArray(MemoryLocation loc, int size) {
    final byte[] arr = new byte[size];
    PlatformDependent.UNSAFE.copyMemory(
      loc.getBaseObject(),
      loc.getBaseOffset(),
      arr,
      BYTE_ARRAY_OFFSET,
      size
    );
    return arr;
  }

  protected byte[] getRandomByteArray(int numWords) {
    assert(numWords > 0);
    final int lengthInBytes = numWords * 8;
    final byte[] bytes = new byte[lengthInBytes];
    rand.nextBytes(bytes);
    return bytes;
  }

  @Test
  public void emptyMap() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    Assert.assertEquals(0, map.size());
    final int keyLengthInWords = 10;
    final int keyLengthInBytes = keyLengthInWords * 8;
    final byte[] key = getRandomByteArray(keyLengthInWords);
    Assert.assertFalse(map.lookup(key, BYTE_ARRAY_OFFSET, keyLengthInBytes).isDefined());
  }

  @Test
  public void setAndRetrieveAKey() {
    BytesToBytesMap map = new BytesToBytesMap(allocator, 64);
    final int recordLengthWords = 10;
    final int recordLengthBytes = recordLengthWords * 8;
    final byte[] keyData = getRandomByteArray(recordLengthWords);
    final byte[] valueData = getRandomByteArray(recordLengthWords);
    try {
      final BytesToBytesMap.Location loc =
        map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes);
      assert(!loc.isDefined());
      loc.storeKeyAndValue(
        keyData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes,
        valueData,
        BYTE_ARRAY_OFFSET,
        recordLengthBytes
      );
      assert(map.lookup(keyData, BYTE_ARRAY_OFFSET, recordLengthBytes).isDefined());
      Assert.assertEquals(recordLengthBytes, loc.getKeyLength());
      Assert.assertEquals(recordLengthBytes, loc.getValueLength());

      final byte[] actualKeyData = getByteArray(loc.getKeyAddress(), recordLengthBytes);
      Assert.assertArrayEquals(keyData, actualKeyData);

      final byte[] actualValueData = getByteArray(loc.getValueAddress(), recordLengthBytes);
      Assert.assertArrayEquals(valueData, actualValueData);

      try {
        loc.storeKeyAndValue(
          keyData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes,
          valueData,
          BYTE_ARRAY_OFFSET,
          recordLengthBytes
        );
        Assert.fail("Should not be able to set a new value for a key");
      } catch (IllegalStateException e) {
        // Expected exception; do nothing.
      }
    } finally {
      map.free();
    }
  }

  @Test
  public void randomizedStressTest() {
    final long size = 65536;
    // Java arrays' hashCodes() aren't based on the arrays' contents, so we need to wrap arrays
    // into ByteBuffers in order to use them as keys here.
    final Map<ByteBuffer, byte[]> expected = new HashMap<ByteBuffer, byte[]>();
    final BytesToBytesMap map = new BytesToBytesMap(allocator, size);

    try {
      // Fill the map to 90% full so that we can trigger probing
      for (int i = 0; i < size * 0.9; i++) {
        final byte[] key = getRandomByteArray(rand.nextInt(256) + 1);
        final byte[] value = getRandomByteArray(rand.nextInt(512) + 1);
        if (!expected.containsKey(ByteBuffer.wrap(key))) {
          expected.put(ByteBuffer.wrap(key), value);
          final BytesToBytesMap.Location loc = map.lookup(
            key,
            BYTE_ARRAY_OFFSET,
            key.length
          );
          assert(!loc.isDefined());
          loc.storeKeyAndValue(
            key,
            BYTE_ARRAY_OFFSET,
            key.length,
            value,
            BYTE_ARRAY_OFFSET,
            value.length
          );
          assert(loc.isDefined());
        }
      }

      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        final byte[] key = entry.getKey().array();
        final byte[] value = entry.getValue();
        final BytesToBytesMap.Location loc = map.lookup(key, BYTE_ARRAY_OFFSET, key.length);
        assert(loc.isDefined());
      }
    } finally {
      map.free();
    }
  }
}
