package bashlog.plan;

import java.util.Collection;

import common.Tools;
import common.plan.node.*;
import common.plan.optimizer.Optimizer;

public class BashlogPlan implements Optimizer {

  /** Adds extra column with dummy value */
  private PlanNode prepareSortCrossProduct(PlanNode p) {
    int[] proj = new int[p.getArity() + 1];
    Comparable<?>[] cnst = new Comparable[proj.length];
    for (int i = 0; i < p.getArity(); i++) {
      proj[i + 1] = i;
    }
    proj[0] = -1;
    cnst[0] = "_";
    return p.project(proj, cnst);
  }

  private PlanNode prepareSortJoin(PlanNode p, int[] columns) {
    if(columns.length == 0) {
      columns = Tools.sequence(p.getArity() + 1);
      Comparable<?>[] cnst = new Comparable<?>[p.getArity() + 1];
      columns[cnst.length - 1] = -1;
      cnst[cnst.length - 1] = "cnst";
      return p.project(columns, cnst);
    }
    if (columns.length == 1) {
      return new SortNode(p, columns);
    }
    CombinedColumnNode c = new CombinedColumnNode(p, columns);
    return new SortNode(c, new int[] { p.getArity() });
  }

  /** Replace certain common.plan.* nodes with their bashlog implementations */
  private PlanNode transform(PlanNode p) {
    if (p instanceof JoinNode) {
      // replace join node with sort join node
      JoinNode joinNode = (JoinNode) p;
      if (joinNode.getLeftProjection().length == 0) {
        // no join condition, so do a cross product
        // sort input and add a dummy column
        PlanNode left = prepareSortCrossProduct(joinNode.getLeft());
        PlanNode right = prepareSortCrossProduct(joinNode.getRight());
        PlanNode crossProduct = new SortJoinNode(left, right, new int[] { 0 }, new int[] { 0 });

        // remove extra columns
        int[] proj = new int[left.getArity() + right.getArity() - 2];
        for (int i = 1; i < left.getArity(); i++) {
          proj[i - 1] = i;
        }
        for (int i = 1; i < right.getArity(); i++) {
          proj[left.getArity() - 2 + i] = left.getArity() + i;
        }
        return crossProduct.project(proj);
      } else {
        // sort input and add combined column if necessary
        PlanNode left = prepareSortJoin(joinNode.getLeft(), joinNode.getLeftProjection());
        PlanNode right = prepareSortJoin(joinNode.getRight(), joinNode.getRightProjection());
        if (joinNode.getLeftProjection().length == 1) {
          // no combined column necessary, so we can directly return the join
          return new SortJoinNode(left, right, joinNode.getLeftProjection(), joinNode.getRightProjection());
        }
        // remove extra columns
        PlanNode join = new SortJoinNode(left, right, new int[] { left.getArity() - 1 }, new int[] { right.getArity() - 1 });
        int rightStart = left.getArity();
        return join.project(Tools.concat(Tools.sequence(left.getArity() - 1), Tools.sequence(rightStart, rightStart + right.getArity() - 1)));
      }

    } else if (p instanceof AntiJoinNode) {
      AntiJoinNode ajn = (AntiJoinNode) p;
      PlanNode left = prepareSortJoin(ajn.getLeft(), ajn.getLeftProjection());
      PlanNode right = prepareSortJoin(ajn.getRight(), Tools.sequence(ajn.getRight().getArity()));

      if (ajn.getLeftProjection().length == 1) {
        // no combined column necessary, so we can directly return the join
        return new SortAntiJoinNode(left, right, ajn.getLeftProjection());
      }
      // remove extra columns
      PlanNode antijoin = new SortAntiJoinNode(left, right.project(new int[] { right.getArity() - 1 }), new int[] { left.getArity() - 1 });
      return antijoin.project(Tools.sequence(left.getArity() - 1));

    } else if (p instanceof RecursionNode) {
      // use sorted recursion
      RecursionNode r = (RecursionNode) p;
      return new RecursionNode(new SortNode(r.getExitPlan(), null), new SortNode(r.getRecursivePlan(), null), r.getDelta(), r.getFull());

    } else if (p instanceof UnionNode) {
      UnionNode u = (UnionNode) p;
      Collection<PlanNode> children = u.children();
      if (children.size() == 0) return u;
      if (children.size() == 1) return children.iterator().next();
      return u;

    }

    return p;
  }

  @Override
  public PlanNode apply(PlanNode t) {
    return t.transform(this::transform);
  }
}
