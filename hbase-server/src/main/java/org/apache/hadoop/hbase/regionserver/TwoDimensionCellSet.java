package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class TwoDimensionCellSet extends CellSkipListSet {

  private final ConcurrentNavigableMap<Cell, ConcurrentNavigableMap<Cell, Cell>> delegatee;
  private CellComparator comparator;
  private final byte[] EMPTY_ROW = {0};


  TwoDimensionCellSet(final CellComparator c) {
    super(c);
    this.comparator = c;
    delegatee = new ConcurrentSkipListMap<>(new Comparator<Cell>() {
      @Override public int compare(Cell o1, Cell o2) {
        return c.compareRows(o1, o2);
      }
    });
  }

  public boolean add(Cell c) {
    ConcurrentNavigableMap<Cell, Cell> previous = delegatee.get(c);
    if (previous == null) {
      ConcurrentNavigableMap<Cell, Cell> empty = new ConcurrentSkipListMap<>(this.comparator);
      previous = delegatee.putIfAbsent(c, empty);
      if (previous == null) {
        previous = empty;
      }
    }
    c=new KeyValue(EMPTY_ROW, 0, 1,
        c.getFamilyArray(), c.getFamilyOffset(), (int)c.getFamilyLength(),
        c.getQualifierArray(), c.getQualifierOffset(), (int) c.getQualifierLength(),
        c.getTimestamp(), KeyValue.Type.codeToType(c.getTypeByte()), c.getValueArray(), c.getValueOffset(),
        c.getValueLength(), c.getTagsArray(), c.getTagsOffset(), c.getTagsLength());
    return previous.put(c,c) == null;
  }
}
