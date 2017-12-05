package bashlog.plan;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.shaded.org.apache.curator.shaded.com.google.common.base.Objects;

import common.plan.PlanNode;

/** Combine several column into one (delimited by \u0001) and append it at the end */
public class CombinedColumnNode implements PlanNode {

  protected final PlanNode child;

  final int[] columns;

  public CombinedColumnNode(PlanNode child, int[] columns) {
    this.child = child;
    this.columns = columns;
    if (columns.length < 2) throw new UnsupportedOperationException("don't use this class if you don't need to!");
  }

  @Override
  public int getArity() {
    return child.getArity() + 1;
  }

  @Override
  public String toString() {
    return operatorString() + "(" + child + ")";
  }

  @Override
  public String operatorString() {
    return "combine_{" + Arrays.toString(columns) + "}";
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(child);
  }

  @Override
  public int hashCode() {
    return child.hashCode() ^ Arrays.hashCode(columns);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof CombinedColumnNode && Objects.equal(child, ((CombinedColumnNode) obj).child)
        && Arrays.equals(columns, ((CombinedColumnNode) obj).columns);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, new CombinedColumnNode(child.transform(fn, this), columns), originalParent);
  }

  public PlanNode getTable() {
    return child;
  }

  public int[] getColumns() {
    return columns;
  }

}
