package common.plan.optimizer;

import common.Tools;
import common.plan.node.JoinNode;
import common.plan.node.PlanNode;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReorderJoinLinear implements Optimizer {

  private static final Logger LOG = LoggerFactory.getLogger(ReorderJoinLinear.class);

  @Override
  public PlanNode apply(PlanNode node) {
    return node.transform((o, n, op) -> {
      if (n instanceof JoinNode && !(PlanNode.parent(op) instanceof JoinNode)) {
        return reorder((JoinNode) n);
      } else {
        return n;
      }
    }, new LinkedList<>());
  }

  Random r = new Random();

  /** Search for index of highest value, which is not yet used
   * @return max, index
   */
  private int[] max(int[] vals, boolean[] available) {
    int max = Integer.MIN_VALUE, maxIdx = -1;
    for (int i = 0; i < vals.length; i++) {
      if (available[i] && vals[i] > max) {
        max = vals[i];
        maxIdx = i;
      }
    }
    return new int[] { max, maxIdx };
  }

  /**
   * Search for index of min value, which is not yet used
   *
   * @return max, index
   */
  @SuppressWarnings("unused")
  private int[] min(int[] vals, boolean[] available) {
    int min = Integer.MAX_VALUE, minIdx = -1;
    for (int i = 0; i < vals.length; i++) {
      if (available[i] && vals[i] < min) {
        min = vals[i];
        minIdx = i;
      }
    }
    return new int[]{min, minIdx};
  }

  /**
   * Search for available index randomly
   *
   * @return max, index
   */
  @SuppressWarnings("unused")
  private int[] rnd(int[] vals, boolean[] available) {
    List<Integer> l = new ArrayList<>();
    for (int i = 0; i < vals.length; i++) {
      if (available[i]) {
        l.add(i);
      }
    }
    int idx = l.get(r.nextInt(l.size()));
    return new int[]{vals[idx], idx};
  }

  /**
   * Reorder joins according to indices in newLeafOrder. Builds a tree like ((leaf0 x leaf1) x leaf2) ...
   *
   * @param newLeaves output parameter, filled by this method
   * @return reordered join plan
   */
  private PlanNode reorder(JoinInfo info, int[] newLeafOrder, List<PlanNode> newLeaves) {
    int[] oldLeafToNewLeaf = Tools.inverse(newLeafOrder);
    PlanNode result = info.leaves.get(newLeafOrder[0]);
    newLeaves.add(result);
    int[] conditionCount = countJoinConditions(info);
    for (int i = 1; i < info.leaves.size(); i++) {
      int oldLeafIdx = newLeafOrder[i];
      int cc = conditionCount[oldLeafIdx];
      int[] prjLeft = new int[cc];
      int[] prjRight = new int[cc];

      int prjIdx = 0;
      for (int[] cond : info.joinConditions) {
        if (cond[0] == oldLeafIdx) {
          int newLeafIdx = oldLeafToNewLeaf[cond[2]];
          if (newLeafIdx < newLeaves.size()) {
            prjLeft[prjIdx] = leafIdxToJoinIdx(newLeaves, newLeafIdx, cond[3]);
            prjRight[prjIdx++] = cond[1];
          }
        } else if (cond[2] == oldLeafIdx) {
          int newLeafIdx = oldLeafToNewLeaf[cond[0]];
          if (newLeafIdx < newLeaves.size()) {
            prjLeft[prjIdx] = leafIdxToJoinIdx(newLeaves, newLeafIdx, cond[1]);
            prjRight[prjIdx++] = cond[3];
          }
        }
      }

      newLeaves.add(info.leaves.get(oldLeafIdx));
      result = result.join(info.leaves.get(oldLeafIdx), //
              Arrays.copyOf(prjLeft, prjIdx), //
              Arrays.copyOf(prjRight, prjIdx));
    }
    return result;
  }

  private PlanNode reorder(JoinNode n) {
    JoinInfo info = new JoinInfo();
    analyze(n, info);

    // reorder joins
    int[] conditionCount = countJoinConditions(info);
    int[] newLeafOrder = orderJoins(info, conditionCount);

    boolean identity = true;
    for (int i = 0; i < newLeafOrder.length; i++) {
      if (newLeafOrder[i] != i) {
        identity = false;
        break;
      }
    }
    if (identity) return n;

    List<PlanNode> newLeaves = new ArrayList<>();
    LOG.debug("new leaf order: " + Arrays.toString(newLeafOrder));
    PlanNode reorderedJoins = reorder(info, newLeafOrder, newLeaves);

    // calculate final projection, to bring columns in right order (the same as n would have produced)
    int[] finalProj = new int[n.getArity()];
    int finalProjIdx = 0; // put next value of finalProj at this index

    int[] oldLeafToNewLeaf = Tools.inverse(newLeafOrder);
    for (int i = 0; i < oldLeafToNewLeaf.length; i++) {
      int newLeafIdx = oldLeafToNewLeaf[i];
      for (int j = 0; j < info.leaves.get(i).getArity(); j++) {
        finalProj[finalProjIdx++] = leafIdxToJoinIdx(newLeaves, newLeafIdx, j);
      }
    }

    return reorderedJoins.project(finalProj);
  }

  private int[] countJoinConditions(JoinInfo info) {
    // count join conditions per leaf node
    int[] useCount = new int[info.leaves.size()];
    for (int[] cond : info.joinConditions) {
      useCount[cond[0]]++;
      useCount[cond[2]]++;
    }
    return useCount;
  }

  /** Indices of the leaf nodes in which they appear after the reordering */
  private int[] orderJoins(JoinInfo info, int[] conditionCount) {
    // heuristic: use relations with lots of join conditions first
    int[] newLeafOrder = new int[conditionCount.length];

    boolean available[] = new boolean[conditionCount.length];
    Arrays.fill(available, true);
    int[] max = max(conditionCount, available);
    available[max[1]] = false;

    int newLeafIdx = 0;
    newLeafOrder[newLeafIdx++] = max[1];
    for (; newLeafIdx < conditionCount.length; newLeafIdx++) {
      boolean availableChildren[] = new boolean[conditionCount.length];
      // determine available children
      for (int[] cond : info.joinConditions) {
        // check whether one of the leaves of the join condition is still available
        // if we still need to join with left plan of condition, check whether right plan of condition is still available
        if (!available[cond[0]]) availableChildren[cond[2]] = available[cond[2]];
        if (!available[cond[2]]) availableChildren[cond[0]] = available[cond[0]];
      }

      max = max(conditionCount, availableChildren);
      // if no children available, check again for all available nodes
      if (max[1] < 0) max = max(conditionCount, available);
      newLeafOrder[newLeafIdx] = max[1];
      available[max[1]] = false;
    }

    return newLeafOrder;
  }

  /**
   * Fill information in JoinInfo
   */
  private void analyze(PlanNode n, JoinInfo info) {
    if (n instanceof JoinNode) {
      JoinNode j = (JoinNode) n;
      analyze(j.getLeft(), info);
      int leftChildren = info.leaves.size();
      analyze(j.getRight(), info);

      int[] leftPrj = j.getLeftProjection();
      int[] rightPrj = j.getRightProjection();
      for (int i = 0; i < leftPrj.length; i++) {
        int[] prj = Tools.concat(
                joinIdxToLeafIdx(info.leaves, 0, leftPrj[i]), //
                joinIdxToLeafIdx(info.leaves, leftChildren, rightPrj[i])
        );
        info.joinConditions.add(prj);
      }
    } else {
      info.leaves.add(n);
    }
  }

  /** Convert leaf index i and its column index j to output index in the join */
  private int leafIdxToJoinIdx(List<PlanNode> newLeaves, int i, int j) {
    for (int k = 0; k < i; k++) {
      j += newLeaves.get(k).getArity();
    }
    return j;
  }

  /**
   * Transform indices of a join node to indices at one of its children
   * @param start index in info.leaves to start search
   * @param colIdx col index at the join node
   * @return [leafIdx, colIdx]
   */
  private int[] joinIdxToLeafIdx(List<PlanNode> leaves, int start, int colIdx) {
    for (int i = start; i < leaves.size(); i++) {
      int arity = leaves.get(i).getArity();
      if (colIdx - arity < 0) {
        return new int[] { i, colIdx };
      }
      colIdx -= arity;
    }
    throw new IllegalStateException();
  }

  class JoinInfo {

    /**
     * First nodes in the subtrees, that are not joins, from left to right
     */
    List<PlanNode> leaves = new ArrayList<>();

    /**
     * Join condition information: [leftLeafIdx, leftColIdx, rightLeafIdx, rightColIdx]
     */
    List<int[]> joinConditions = new ArrayList<>();
  }

}
