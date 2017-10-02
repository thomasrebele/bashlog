package bashlog.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import common.plan.PlanNode;

public class SortNode implements PlanNode {

  private final PlanNode child;

  private final int[] sortColumns;

  public SortNode(PlanNode child, int[] sortColumns) {
    this.child = child;
    if (sortColumns == null) {
      this.sortColumns = new int[child.getArity()];
      for (int i = 0; i < this.sortColumns.length; i++) {
        this.sortColumns[i] = i;
      }
    } else {
    this.sortColumns = sortColumns;
    }
  }

  @Override
  public int getArity() {
    return child.getArity();
  }

  @Override
  public String toString() {
    return operatorString() + "(" + child + ")";
  }

  @Override
  public String operatorString() {
    return "sort_{" + Arrays.toString(sortColumns) + "}";
  }

  @Override
  public List<PlanNode> args() {
    return Arrays.asList(child);
  }

  public int[] sortColumns() {
    return sortColumns;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SortNode && Objects.equals(child, ((SortNode) obj).child) && Objects.equals(sortColumns, ((SortNode) obj).sortColumns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, sortColumns);
  }
}
