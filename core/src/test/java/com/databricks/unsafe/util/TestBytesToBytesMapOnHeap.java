package com.databricks.unsafe.util;

import com.databricks.unsafe.util.memory.MemoryAllocator;

public class TestBytesToBytesMapOnHeap extends AbstractTestBytesToBytesMap {

  @Override
  protected MemoryAllocator getMemoryAllocator() {
    return MemoryAllocator.HEAP;
  }

}
