package common.plan.optimizer;

import common.Tools;
import common.plan.node.*;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Push down filter and projects nodes as much as possible. This might remove rows from intermediate results, and thus reducing their size. */
public class PushDownFilterAndProject implements Optimizer {

  /**
   * Takes for input a simplified tree
   */
  @Override
  public PlanNode apply(PlanNode node) {
    return node.transform((n) -> {
      if (n instanceof ConstantEqualityFilterNode) {
        return optimize((ConstantEqualityFilterNode) n);
      } else if (n instanceof ProjectNode) {
          return optimize((ProjectNode) n);
      } else {
        return n;
      }
    });
  }

  private PlanNode optimize(ConstantEqualityFilterNode node) {
    PlanNode child = node.getTable();
    if (child instanceof ProjectNode) {
      return swap(node, (ProjectNode) child);
    } else if (child instanceof UnionNode) {
      return swap(node, (UnionNode) child);
    } else if (child instanceof JoinNode) {
      return swap(node, (JoinNode) child);
    } else if (child instanceof RecursionNode) {
      return swap(node, (RecursionNode) child);
    } else {
      return node;
    }
  }

  private PlanNode optimize(ProjectNode node) {
    PlanNode child = node.getTable();
    if (child instanceof JoinNode) {
      return swap(node, (JoinNode) child);
    } else if (child instanceof ProjectNode) {
      return SimplifyPlan.mergeProjections(node, (ProjectNode) child);
    } else if (child instanceof UnionNode) {
      return swap(node, (UnionNode) child);
    } else {
      return node;
    }
  }

  private PlanNode swap(ConstantEqualityFilterNode filter, RecursionNode recursion) {
    // only push to recursive plan, if selection arrives at all delta nodes (and occurs at least one more time)!
    PlanNode newExit = newEqualityFilter(recursion.getExitPlan(), filter.getField(), filter.getValue());
    PlanNode newRecursion = apply(recursion.getRecursivePlan().equalityFilter(filter.getField(), filter.getValue()));

    // check whether all delta nodes have the pushed down select
    // count is increased for every node delta = recursion.getDelta()
    // count is decreased for every filter(delta) combination

    PlanNode selectionDelta = new ConstantEqualityFilterNode(recursion.getDelta(), filter.getField(), filter.getValue());

    int count[] = new int[] { 0 };
    Optimizer check = root -> root.transform(t -> {
      if (selectionDelta.equals(t)) {
        count[0]--;
        return recursion.getDelta();
      }
      if (recursion.getDelta().equals(t)) {
        count[0]++;
      }
      return t;
    });

    newRecursion = check.apply(newRecursion);
    // if all delta nodes have the pushed down select
    if (count[0] == 0) {
      return recursion.transform(newExit, newRecursion);
    }
    return filter;
  }

  private PlanNode swap(ConstantEqualityFilterNode filter, ProjectNode project) {
    int newField = project.getProjection()[filter.getField()];
    if (newField >= 0) { //We could move the project
      return newEqualityFilter(project.getTable(), newField, filter.getValue()).project(
              project.getProjection(),
              project.getConstants()
      );
    } else {
      boolean sameConstant = project.getConstant(filter.getField()).filter(constant -> constant.equals(filter.getValue())).isPresent();
      if (sameConstant) {
        return project;
      } else {
        return PlanNode.empty(project.getArity());
      }
    }
  }

  private PlanNode swap(ConstantEqualityFilterNode filter, UnionNode union) {
    return new UnionNode(
        union.getChildren().stream().map(child -> newEqualityFilter(child, filter.getField(), filter.getValue())).collect(Collectors.toSet()),
        filter.getArity());
  }

  private PlanNode swap(ConstantEqualityFilterNode filter, JoinNode join) {
    int leftField = join.getLeftField(filter.getField());
    int rightField = join.getRightField(filter.getField());

    return new JoinNode(
        (leftField >= 0 ? newEqualityFilter(join.getLeft(), leftField, filter.getValue()) : join.getLeft()), //
        (rightField >= 0 ? newEqualityFilter(join.getRight(), rightField, filter.getValue()) : join.getRight()), //
        join.getLeftJoinProjection(), join.getRightJoinProjection());
  }

  private PlanNode swap(ProjectNode node, JoinNode child) {
    boolean[] neededLeft = new boolean[child.getLeft().getArity()], neededRight = new boolean[child.getRight().getArity()];
    Arrays.fill(neededLeft, false);
    Arrays.fill(neededRight, false);
    for (int i : child.getLeftJoinProjection()) {
      neededLeft[i] = true;
    }
    for (int i : child.getRightJoinProjection()) {
      neededRight[i] = true;
    }
    for (int i : node.getProjection()) {
      if (i < child.getLeft().getArity()) {
        neededLeft[i] = true;
      } else {
        neededRight[i - child.getLeft().getArity()] = true;
      }
    }

    int prjLeft[] = boolToIndex(neededLeft);
    int prjRight[] = boolToIndex(neededRight);
    int oldLeftToNew[] = Tools.inverse(prjLeft, child.getLeft().getArity());
    int oldRightToNew[] = Tools.inverse(prjRight, child.getRight().getArity());

    int newPrj[] = new int[node.getArity()];
    for (int i = 0; i < node.getProjection().length; i++) {
      int dst = node.getProjection()[i];
      if (dst < child.getLeft().getArity()) {
        newPrj[i] = oldLeftToNew[dst];
      } else {
        newPrj[i] = prjLeft.length + oldRightToNew[dst - child.getLeft().getArity()];
      }
    }
    PlanNode left = Tools.isIdentityProjection(prjLeft, child.getLeft().getArity()) ? child.getLeft()
        : child.getLeft().project(prjLeft);
    PlanNode right = Tools.isIdentityProjection(prjRight, child.getRight().getArity()) ? child.getRight()
        : child.getRight().project(prjRight);

    return new JoinNode(
            left, right,
            Tools.apply(child.getLeftJoinProjection(), oldLeftToNew),
            Tools.apply(child.getRightJoinProjection(), oldRightToNew)
    ).project(newPrj);
  }

  private int[] boolToIndex(boolean[] used) {
    int indices[] = new int[Tools.count(used, true)];
    int j = 0;
    for (int i = 0; i < used.length; i++) {
      if (used[i]) {
        indices[j++] = i;
      }
    }
    return indices;
  }

  private PlanNode swap(ProjectNode projection, UnionNode union) {
    return new UnionNode(union.getChildren().stream().map(child ->
            apply(newProjection(child, projection.getProjection(), projection.getConstants())))
    );
  }

  private PlanNode newProjection(PlanNode node, int[] fields, Comparable<?>[] constants) {
    return apply(node.project(fields, constants));
  }

  private PlanNode newEqualityFilter(PlanNode node, int field, Comparable<?> value) {
    return apply(node.equalityFilter(field, value));
  }
}
