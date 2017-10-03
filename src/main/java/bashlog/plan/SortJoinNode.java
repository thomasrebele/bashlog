package bashlog.plan;

import java.util.function.Function;

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
  public PlanNode transform(Function<PlanNode, PlanNode> fn) {
    return fn.apply(new SortJoinNode(getLeft().transform(fn), getRight().transform(fn), getLeftJoinProjection(), getRightJoinProjection()));
  }

}
