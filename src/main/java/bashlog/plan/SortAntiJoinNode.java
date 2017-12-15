package bashlog.plan;

import java.util.Arrays;

import common.Tools;
import common.plan.node.AntiJoinNode;
import common.plan.node.PlanNode;

/** Anti-Join two sorted inputs based on ONE column */
public class SortAntiJoinNode extends SortJoinNode {

  private final int[] outputProjection;

  public SortAntiJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection) {
    this(left, right, leftJoinProjection, Tools.sequence(left.getArity() ));
  }

  public SortAntiJoinNode(PlanNode left, PlanNode right, int[] leftJoinProjection, int[] outputProjection) {
    super(left, right, leftJoinProjection, new int[] { 0 });
    if (leftJoinProjection.length > 1) throw new UnsupportedOperationException("sort join does not support sorting on more than one column");
    this.outputProjection = outputProjection;
  }

  @Override
  public String operatorString() {
    return super.operatorString().replaceFirst("_", "_sort_") + " out " + Arrays.toString(outputProjection);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this,
        new SortAntiJoinNode(getLeft().transform(fn, this), getRight().transform(fn, this), getLeftProjection(),
            outputProjection),
        originalParent);
  }

  public int[] getOutputProjection() {
    return outputProjection;
  }

  @Override
  public int getArity() {
    return outputProjection.length;
  }

}
