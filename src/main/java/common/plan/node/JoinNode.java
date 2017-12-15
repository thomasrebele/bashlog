package common.plan.node;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Does join on fields such that the projection of left with leftProjection equals the rightProjection of right
 */
public class JoinNode implements PlanNode {

  private final PlanNode left;

  private final PlanNode right;

  private final int[] leftProjection;

  private final int[] rightProjection;

  public JoinNode(PlanNode left, PlanNode right, int[] leftProjection, int[] rightProjection) {
    if (leftProjection.length != rightProjection.length) {
      throw new IllegalArgumentException("The left and right projections for join should have the same size");
    }
    if (Arrays.stream(leftProjection).anyMatch(i -> i >= left.getArity())) {
      throw new IllegalArgumentException("Invalid left projection: trying to project a non-existing field");
    }
    if (Arrays.stream(rightProjection).anyMatch(i -> i >= right.getArity())) {
      throw new IllegalArgumentException("Invalid left projection: trying to project a non-existing field");
    }

    this.left = left;
    this.right = right;
    this.leftProjection = leftProjection;
    this.rightProjection = rightProjection;
  }

  public PlanNode getLeft() {
    return left;
  }

  public PlanNode getRight() {
    return right;
  }

  public int[] getLeftProjection() {
    return leftProjection;
  }

  public int[] getRightProjection() {
    return rightProjection;
  }

  @Override
  public int getArity() {
    return left.getArity() + right.getArity();
  }

  @Override
  public String toString() {
    return left.toString() + operatorString() + right.toString();
  }

  @Override
  public String operatorString() {
    return "⋈_{" + IntStream.range(0, leftProjection.length).mapToObj(i -> leftProjection[i] + "=" + rightProjection[i])
        .collect(Collectors.joining(", ")) + "}";
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(left, right);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    JoinNode node = (JoinNode) obj;
    return (left.equals(node.left) && right.equals(node.right) && Arrays.equals(leftProjection, node.leftProjection) && Arrays.equals(rightProjection, node.rightProjection));
  }

  @Override
  public int hashCode() {
    return left.hashCode() ^ right.hashCode() ^ Arrays.hashCode(leftProjection) ^ Arrays.hashCode(rightProjection);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, new JoinNode(left.transform(fn, this), right.transform(fn, this), leftProjection, rightProjection), originalParent);
  }

  /** Transform index of output column to index of left input column, or -1 if not possible */
  public int getLeftField(int joinField) {
    if (joinField < 0) return -1;
    if (joinField < left.getArity()) return joinField;
    return translate(joinField - left.getArity(), rightProjection, leftProjection);
  }

  /** Transform index of output column to index of right input column, or -1 if not possible */
  public int getRightField(int joinField) {
    if (joinField < 0) return -1;
    int leftArity = left.getArity();
    if (leftArity <= joinField && joinField < leftArity + right.getArity()) return joinField - leftArity;
    return translate(joinField, leftProjection, rightProjection);
  }

  private int translate(int field, int[] src, int[] dst) {
    for (int i = 0; i < src.length; i++) {
      if (src[i] == field) return dst[i];
    }
    return -1;
  }
}
