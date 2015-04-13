package com.databricks.unsafe.util;

/**
 * Helper class for calculating allocation sizes and relative offsets
 * when constructing rows.
 */
public class RowSizeCalculator {
  private int numFields = 0;
  private int variableLengthSizeInWords = 0;

  public void addLongField() {
    numFields++;
  }

  public void addFloatField() {
    numFields++;
  }

  public int getBitsetSizeInWords() {
    return ((numFields / 64) + 1);
  }

  public int getFixedLengthSizeInWords() {
    return numFields;
  }

  public void addStringField(long lengthInBytes) {
    numFields++;
    final int paddedSizeInWords = (int) (lengthInBytes / 8 + (lengthInBytes % 8 == 0 ? 0 : 1));
    // Include an extra word for storing the length of the string in bytes.
    variableLengthSizeInWords = 1 + paddedSizeInWords;
  }

  public int getRowSizeInWords() {
    return getBitsetSizeInWords() + getFixedLengthSizeInWords() + variableLengthSizeInWords;
  }
}
