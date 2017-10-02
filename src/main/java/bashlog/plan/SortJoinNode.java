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
}
