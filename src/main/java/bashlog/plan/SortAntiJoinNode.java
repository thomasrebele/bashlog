package bashlog.plan;

import common.Tools;
import common.plan.node.PlanNode;

/** Anti-Join two sorted inputs based on ONE column */
public class SortAntiJoinNode extends SortJoinNode {

  public SortAntiJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection) {
    this(left, right, leftJoinProjection, Tools.sequence(left.getArity() ));
  }

  public SortAntiJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection, int[] outputProjection) {
    super(left, right, leftJoinProjection, leftJoinProjection.length == 1 ? new int[] { 0 } : new int[] {}, outputProjection);
    if (leftJoinProjection.length > 1) throw new UnsupportedOperationException("sort join does not support sorting on more than one column");
  }

  @Override
  public String operatorString() {
    return super.operatorString().replaceFirst("⋈_", "▷_");
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this,
        new SortAntiJoinNode(getLeft().transform(fn, this), getRight().transform(fn, this), getLeftProjection(),
            outputProjection),
        originalParent);
  }

}
