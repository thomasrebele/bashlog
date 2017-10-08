package bashlog.plan;

import common.plan.JoinNode;
import common.plan.PlanNode;

public class SortJoinNode extends JoinNode {

  public SortJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection, int[] rightJoinProjection) {
    super(left, right, leftJoinProjection, rightJoinProjection);
  }

  @Override
  public String operatorString() {
    return "sort_" + super.operatorString();
  }

  @Override
  public PlanNode transform(Transform fn, PlanNode oldParent) {
    return fn.apply(this,
        new SortJoinNode(getLeft().transform(fn, this), getRight().transform(fn, this), getLeftJoinProjection(), getRightJoinProjection()), oldParent);
  }

}
