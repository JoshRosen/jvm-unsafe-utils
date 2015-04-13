package com.databricks.unsafe.util;

import org.junit.Assert;
import org.junit.Test;

import com.databricks.unsafe.util.memory.MemoryLocation;
import com.databricks.unsafe.util.memory.MemoryBlock;

public class TestRow {

  final static long WORD_SIZE = 8;

  @Test
  public void basicRowCreationTest() {
    // We're going to create a row with two fields, a long and a string ("Hello, World!")
    final String javaStr = "Hello, World!";
    final byte[] javaStrBytes = javaStr.getBytes();

    // Calculate how much space we should allocate for the row
    final RowSizeCalculator sizeCalculator = new RowSizeCalculator();
    sizeCalculator.addLongField();
    sizeCalculator.addStringField(javaStrBytes.length);
    final int rowSizeInWords = sizeCalculator.getRowSizeInWords();
    final MemoryLocation memory = MemoryBlock.fromLongArray(new long[rowSizeInWords]);
    // Note that in real code, we might be re-using a region of memory and thus would have to
    // zero out the bitset and other regions of memory.
    final Row row = new Row(memory, 2);
    row.putLong(0, 42);
    // Compute the relative offset, in words, of the start of the string.
    long stringStartOffsetInWords =
      sizeCalculator.getBitsetSizeInWords() + sizeCalculator.getFixedLengthSizeInWords();
    // Store this offset in the fixed-length section
    row.putLong(1, stringStartOffsetInWords);
    // Store the string in the variable-length section
    UTF8StringMethods.createFromJavaString(
      memory.getBaseObject(),
      memory.getBaseOffset() + stringStartOffsetInWords * WORD_SIZE,
      javaStr);
    // At this point, the data in the row should be laid out correctly, so let's try to read it back
    Assert.assertEquals(42, row.getLong(0));
    UTF8StringPointer stringFromRow = new UTF8StringPointer();
    row.getString(1, stringFromRow);
    Assert.assertEquals(javaStr, stringFromRow.toJavaString());
  }
}
