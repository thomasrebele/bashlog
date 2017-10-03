package bashlog.plan;

import java.util.function.BiFunction;

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
  public PlanNode transform(BiFunction<PlanNode, PlanNode[], PlanNode> fn) {
    PlanNode newLeft = getLeft().transform(fn);
    PlanNode newRight = getRight().transform(fn);
    PlanNode newNode = fn.apply(this, new PlanNode[] { newLeft, newRight });
    if (newNode != null) return newNode;
    return new SortJoinNode(newLeft, newRight, getLeftJoinProjection(), getRightJoinProjection());
  }
}
