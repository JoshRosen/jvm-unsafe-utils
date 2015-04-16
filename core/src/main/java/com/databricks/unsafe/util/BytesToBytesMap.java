/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;
import com.databricks.unsafe.util.memory.MemoryBlock;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A bytes to bytes hash map where keys and values are contiguous regions of bytes.
 *
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 *
 * Note that even though we use long for indexing, the map can support up to 2^31 keys because
 * we use 32 bit MurmurHash. In either case, if the key cardinality is so high, you should probably
 * be using sorting instead of hashing for better cache locality.
 */
public final class BytesToBytesMap {

  private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  /** Bit mask for the lower 37 bits of a long. */
  private static final long MASK_LONG_LOWER_37_BITS = 0x1FFFFFFFFFL;
  // 0b11111_11111111_11111111_11111111_11111111L;

  /** Bit mask for the upper 27 bits of a long. */
  private static final long MASK_LONG_UPPER_27_BITS = ~MASK_LONG_LOWER_37_BITS;

  /** Bit mask for the upper 27 bits of an int, i.e. bit 5 - 31 (inclusive) for a long. */
  private static final int MASK_INT_UPPER_27_BITS;
  // 0b11111111_11111111_11111111_11100000;

  static {
    MASK_INT_UPPER_27_BITS = ((1 << 27) - 1) << 5;
  }

  private final MemoryAllocator allocator;

  /**
   * A linked list for tracking all allocated data pages so that we can free all of our memory.
   */
  private final List<MemoryBlock> dataPages = new LinkedList<MemoryBlock>();

  /**
   * A single array to store the key and value.
   *
   * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
   * while position {@code 2 * i + 1} in the array holds the upper bits of the key's hashcode plus
   * the relative offset from the key pointer to the value at index {@code i}.
   */
  private LongArray longArray;

  /**
   * A {@link BitSet} used to track location of the map where the key is set.
   * Size of the bitset should be half of the size of the long array.
   */
  private BitSet bitset;

  private final double loadFactor;

  /**
   * Number of keys defined in the map.
   */
  private long size;

  private long growthThreshold;

  private long mask;

  private final Location loc;


  public BytesToBytesMap(MemoryAllocator allocator, long initialCapacity, double loadFactor) {
    this.allocator = allocator;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    allocate(initialCapacity);
  }

  public BytesToBytesMap(MemoryAllocator allocator, long initialCapacity) {
    this(allocator, initialCapacity, 0.70);
  }

  /**
   * Returns the number of keys defined in the map.
   */
  public long size() { return size; }

  /**
   * Updates the value the key maps to.
   */
  public void put(
      Object keyBaseObject,
      long keyBaseOffset,
      long keyRowLength,
      Object valueBaseObject,
      long valueBaseOffset,
      long valueRowLength) {
    // TODO
  }

  /**
   * Returns true if the key is defined in this map.
   */
//  public boolean containsKey(long key) {
//    return lookup(key).isDefined();
//  }

  /**
   * Updates the value the key maps to.
   */
//  public void put(long key, long value) {
//    lookup(key).setValue(value);
//  }

  /**
   * Returns the value to which the specified key is mapped. In the case the key is not defined,
   * this has undefined behavior.
   */
//  public long get(long key) {
//    return lookup(key).getValue();
//  }

  /**
   * Looks up a key, and return a {@link Location} handle that can be used to test existence
   * and read/write values.
   *
   * This function always return the same {@link Location} instance to avoid object allocation.
   */
  public Location lookup(
      Object keyBaseObject,
      long keyBaseOffset,
      int keyRowLengthBytes) {

    final long hashcode = HASHER.hashUnsafeWords(keyBaseObject, keyBaseOffset, keyRowLengthBytes);
    final long pos = hashcode & mask;
    long partialKeyHashCode = (hashcode & MASK_INT_UPPER_27_BITS) >> 5;
    long step = 1;
    while (true) {
      if (!bitset.isSet(pos)) {
        // This is a new key.
        // TODO: -1 is wrong here
        return loc.with(pos, -1, false);
      } else {
        long stored = longArray.get(pos * 2);
        if (((stored & MASK_LONG_UPPER_27_BITS) >> 27) == partialKeyHashCode) {
          // Partial hash code matches. There is a high likelihood this is the place.
          long pointer = stored & MASK_LONG_LOWER_37_BITS;
          // TODO: -1 is wrong here
          return loc.with(pos, -1, true);
        }
      }
      //pos = (pos + step) & mask;
      step++;
    }
  }

  /**
   * Handle returned by {@link BytesToBytesMap#lookup(Object, long, int)} function.
   */
  public final class Location {
    private long pos;
    private long key;
    private boolean isDefined;

    Location with(long pos, long key, boolean isDefined) {
      this.pos = pos;
      this.key = key;
      this.isDefined = isDefined;
      return this;
    }

    /**
     * Returns true if the key is defined at this position, and false otherwise.
     */
    public boolean isDefined() {
      return isDefined;
    }

    /**
     * Returns the key defined at this position. Unspecified behavior if the key is not defined.
     */
    public long getKey() {
      return longArray.get(pos * 2);
    }

    /**
     * Returns the value defined at this position. Unspecified behavior if the key is not defined.
     */
    public long getValue() {
      return longArray.get(pos * 2 + 1);
    }

    /**
     * Updates the value defined at this position. Unspecified behavior if the key is not defined.
     */
    public void setValue(long value) {
      if (!isDefined) {
        size++;
        bitset.set(pos);
        longArray.set(pos * 2, key);
      }
      longArray.set(pos * 2 + 1, value);
    }
  }

  private void allocate(long capacity) {
    capacity = Math.max(nextPowerOf2(capacity), 64);
    longArray = new LongArray(allocator.allocate(capacity * 8 * 2));
    bitset = new BitSet(allocator.allocate(capacity / 8));

    this.growthThreshold = (long) (capacity * loadFactor);
    this.mask = capacity - 1;
  }

  /**
   * Free all allocated memory associated with this map, including the storage for keys and values
   * as well as the hash map array itself.
   */
  public void free() {
    allocator.free(longArray.memoryBlock());
    longArray = null;
    allocator.free(bitset.memoryBlock());
    bitset = null;
    Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
    while (dataPagesIterator.hasNext()) {
      allocator.free(dataPagesIterator.next());
      dataPagesIterator.remove();
    }
    assert(dataPages.isEmpty());
  }

  /**
   * Grows the size of the hash table and re-hash everything.
   */
  private void growAndRehash() {
    // Store references to the old data structures to be used when we re-hash
    final LongArray oldLongArray = longArray;
    final BitSet oldBitSet = bitset;
    final long oldCapacity = oldBitSet.capacity();

    // Allocate the new data structures
    allocate(growthStrategy.nextCapacity(oldCapacity));

    // Re-hash
    for (long pos = oldBitSet.nextSetBit(0); pos >= 0; pos = oldBitSet.nextSetBit(pos + 1)) {
      final long key = oldLongArray.get(pos * 2);
      final long value = oldLongArray.get(pos * 2 + 1);
      long newPos = HASHER.hashLong(key) & mask;
      long step = 1;
      boolean keepGoing = true;

      // No need to check for equality here when we insert so this has one less if branch than
      // the similar code path in addWithoutResize.
      while (keepGoing) {
        if (!bitset.isSet(newPos)) {
          longArray.set(newPos * 2, key);
          longArray.set(newPos * 2 + 1, value);
          bitset.set(newPos);
          keepGoing = false;
        } else {
          newPos = (newPos + step) & mask;
          step++;
        }
      }
    }

    // Deallocate the old data structures.
    allocator.free(oldLongArray.memoryBlock());
    allocator.free(oldBitSet.memoryBlock());
  }

  /** Returns the next number greater or equal num that is power of 2. */
  private long nextPowerOf2(long num) {
    final long highBit = Long.highestOneBit(num);
    return (highBit == num) ? num : highBit << 1;
  }
}
