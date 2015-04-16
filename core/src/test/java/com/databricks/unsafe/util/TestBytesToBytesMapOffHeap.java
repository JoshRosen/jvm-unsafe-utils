package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;

public class TestBytesToBytesMapOffHeap extends AbstractTestBytesToBytesMap {

  @Override
  protected MemoryAllocator getMemoryAllocator() {
    return MemoryAllocator.UNSAFE;
  }

}
