package bashlog.plan;

import java.util.*;

import common.Tools;
import common.plan.node.PlanNode;

public class SortNode implements PlanNode {

  private final PlanNode child;

  private final int[] sortColumns;

  public SortNode(PlanNode child, int[] sortColumns) {
    this.child = child;
    this.sortColumns = sortColumns == null ? Tools.sequence(child.getArity()) : sortColumns;
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
    return toPrettyString(); //operatorString() + "(" + child + ")";
  }

  @Override
  public String operatorString() {
    return "sort_{" + Arrays.toString(sortColumns) + "}";
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(child);
  }

  public int[] sortColumns() {
    return sortColumns;
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if (this == obj) return true;
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    SortNode node = (SortNode) obj;
    return Arrays.equals(sortColumns, node.sortColumns) && child.equals(node.child, assumedEqualities);
  }

  @Override
  public int hashCode() {
    return Objects.hash(child, Arrays.hashCode(sortColumns));
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, new SortNode(child.transform(fn, this), sortColumns), originalParent);
  }

}
