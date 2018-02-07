package bashlog.plan;

import common.Tools;
import common.plan.node.PlanNode;
import common.plan.node.ProjectNode;
import common.plan.node.RecursionNode;
import common.plan.optimizer.Optimizer;

import java.util.Arrays;

// TODO: "push up" sorting (only useful on topmost sort?)
public class BashlogOptimizer implements Optimizer {

  public PlanNode apply(PlanNode node) {
    return node.transform((n) -> {
      if (n instanceof SortNode) {
        SortNode sn = (SortNode) n;
        int[] parentSortCols = sn.sortColumns();
        int[] childSortCols = null;

        if (sn.getTable() instanceof RecursionNode) {
          childSortCols = Tools.sequence(sn.getTable().getArity());
        } else if (sn.getTable() instanceof SortNode) {
          childSortCols = ((SortNode) sn.getTable()).sortColumns();
        }

        if (childSortCols != null && parentSortCols.length <= childSortCols.length) {
          for(int i=0; i<parentSortCols.length; i++) {
            if (parentSortCols[i] != childSortCols[i]) {
              return n;
            }
          }
          if (sn.getTable() instanceof RecursionNode) {
            return sn.getTable();
          } else if (sn.getTable() instanceof SortNode) {
            return ((SortNode) n).getTable();
          }
        }
      }

      else if (n instanceof ProjectNode) {
        ProjectNode p = (ProjectNode) n;
        // merge proj(sort(...)) together
        if (p.getTable() instanceof SortJoinNode && !p.hasConstants()) {
          int[] projection = Arrays.copyOf(p.getProjection(), p.getProjection().length);
          SortJoinNode sj = (SortJoinNode) p.getTable();
          for (int i = 0; i < projection.length; i++) {
            projection[i] = sj.getOutputProjection()[projection[i]];
          }
          return new SortJoinNode(sj.getLeft(), sj.getRight(), sj.getLeftProjection(), sj.getRightProjection(), projection);
        }
      }
      return n;
    });
  }

  }
