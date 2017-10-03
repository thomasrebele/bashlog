package bashlog.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import common.plan.PlanNode;

public class SortNode implements PlanNode {

  private final PlanNode child;

  private final int[] sortColumns;

  public SortNode(PlanNode child, int[] sortColumns) {
    this.child = child;
    this.sortColumns = sortColumns;
  }

  @Override
  public int getArity() {
    return child.getArity();
  }

  public PlanNode getTable() {
    return child;
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

  @Override
  public PlanNode transform(Function<PlanNode, PlanNode> fn) {
    return fn.apply(new SortNode(child.transform(fn), sortColumns));
  }

}
