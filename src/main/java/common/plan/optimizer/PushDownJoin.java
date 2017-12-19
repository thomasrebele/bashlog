package common.plan.optimizer;

import common.Tools;
import common.plan.node.*;

import java.util.Arrays;
import java.util.OptionalInt;

/** Push down join nodes over selection and projection. This groups joins together, in order to reorder them. */
public class PushDownJoin implements Optimizer {

  private final boolean LEFT = false, RIGHT = true;

  /**
   * Takes for input a simplified tree
   */
  @Override
  public PlanNode apply(PlanNode node) {
    return node.transform((n) -> {
      if (n instanceof JoinNode) {
        return optimize((JoinNode) n);
      } else {
        return n;
      }
    });
  }

  private PlanNode optimize(JoinNode joinNode) {
    PlanNode leftChild = joinNode.getLeft();
    if (leftChild instanceof ProjectNode) {
      return swap(joinNode, (ProjectNode) leftChild, LEFT);
    } else if (leftChild instanceof ConstantEqualityFilterNode) {
      return swap(joinNode, (ConstantEqualityFilterNode) leftChild, LEFT);
    } else if (leftChild instanceof VariableEqualityFilterNode) {
      return swap(joinNode, (VariableEqualityFilterNode) leftChild, LEFT);
    }

    PlanNode rightChild = joinNode.getRight();
    if (rightChild instanceof ProjectNode) {
      return swap(joinNode, (ProjectNode) rightChild, RIGHT);
    } else if (rightChild instanceof ConstantEqualityFilterNode) {
      return swap(joinNode, (ConstantEqualityFilterNode) rightChild, RIGHT);
    } else if (rightChild instanceof VariableEqualityFilterNode) {
      return swap(joinNode, (VariableEqualityFilterNode) rightChild, RIGHT);
    }

    return joinNode;
  }

  private PlanNode swap(JoinNode joinNode, VariableEqualityFilterNode filter, boolean direction) {
    if (direction == LEFT) {
      return filter.getTable().join(joinNode.getRight(), joinNode.getLeftProjection(), joinNode.getRightProjection())
          .equalityFilter(filter.getField1(), filter.getField2());
    } else {
      return joinNode.getLeft().join(filter.getTable(), joinNode.getLeftProjection(), joinNode.getRightProjection())
          .equalityFilter(joinNode.getLeft().getArity() + filter.getField1(), joinNode.getLeft().getArity() + filter.getField2());
    }
  }

  private PlanNode swap(JoinNode joinNode, ConstantEqualityFilterNode filter, boolean direction) {
    if(direction == LEFT) {
      return filter.getTable().join(joinNode.getRight(), joinNode.getLeftProjection(), joinNode.getRightProjection())
          .equalityFilter(filter.getField(), filter.getValue());
    }
    else {
      return joinNode.getLeft().join(filter.getTable(), joinNode.getLeftProjection(), joinNode.getRightProjection())
          .equalityFilter(joinNode.getLeft().getArity() + filter.getField(), filter.getValue());
    }
  }

  private PlanNode swap(JoinNode joinNode, ProjectNode proj, boolean direction) {
    // default parameters for join
    PlanNode left = joinNode.getLeft(), right = joinNode.getRight();
    int[] joinFieldLeft = joinNode.getLeftProjection(), joinFieldRight = joinNode.getRightProjection();

    if (direction == LEFT) {
      left = proj.getTable();
    } else {
      right = proj.getTable();
    }

    // projection fields of new projection
    int[] prjFields;
    int leftArity = left.getArity(), rightArity = right.getArity();
    if (direction == LEFT) {
      prjFields = Tools.concat(proj.getProjection(), Tools.sequence(leftArity, leftArity + rightArity));
      joinFieldLeft = Tools.apply(joinFieldLeft, proj.getProjection());
    } else {
      prjFields = Tools.concat(Tools.sequence(0, leftArity), Tools.addToElements(proj.getProjection(), leftArity));
      joinFieldRight = Tools.apply(joinFieldRight, proj.getProjection());
    }

    // projection constants of new projection
    Comparable<?>[] constants = null;
    if (proj.hasConstants()) {
      if (direction == LEFT) {
        constants = Tools.concat(proj.getConstants(), new Comparable[rightArity]);
      } else {
        constants = Tools.concat(new Comparable[leftArity], proj.getConstants());
      }
    }

    // create final join and projection 
    PlanNode result = apply(left.join(right, joinFieldLeft, joinFieldRight));
    if (proj.hasConstants()) {
      result = result.project(prjFields, constants);
    } else {
      result = result.project(prjFields);
    }
    return result;
  }


}
