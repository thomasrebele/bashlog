package bashlog.plan;

import common.plan.JoinNode;
import common.plan.PlanNode;

/** Join two sorted inputs based on ONE column */
public class SortJoinNode extends JoinNode {

  public SortJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection, int[] rightJoinProjection) {
    super(left, right, leftJoinProjection, rightJoinProjection);
    if (leftJoinProjection.length > 1) throw new UnsupportedOperationException("sort join does not support sorting on more than one column");
    if (leftJoinProjection.length != rightJoinProjection.length) throw new UnsupportedOperationException("join requires one column on each child");
  }

  @Override
  public String operatorString() {
    return super.operatorString().replaceFirst("_", "_sort_");
  }

  @Override
  public PlanNode transform(Transform fn, PlanNode oldParent) {
    return fn.apply(this,
        new SortJoinNode(getLeft().transform(fn, this), getRight().transform(fn, this), getLeftJoinProjection(), getRightJoinProjection()), oldParent);
  }

}
