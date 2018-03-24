package common.plan.optimizer;

import java.util.*;
import java.util.stream.Collectors;

import common.plan.node.*;

/**
 * Create multi output nodes in a plan.
 */
public class MultiOutput implements Optimizer {

  // leaf node to info
  private HashMap<PlanNode, Info> planToInfo;

  private HashMap<PlaceholderNode, PlanNode> placeholderToParent = new HashMap<>();

  @Override
  public PlanNode apply(PlanNode t) {
    planToInfo = new HashMap<>();
    // inner node to placeholder
    HashMap<PlanNode, PlanNode> nodesToReuseNode = new HashMap<>();

    // determine for which leaf nodes shall become a multi output
    analyzeStructure(t, 0);
    planToInfo.forEach((pn, i) -> {
      i.createMultiOutputNode = i.plans.size() > 1;
    });

    HashMap<PlanNode, List<Info>> nodesToInfo = new HashMap<>();
    planToInfo.forEach((p, i) -> {
      if (i.createMultiOutputNode) {
        PlanNode reuseAt = p instanceof PlaceholderNode ? placeholderToParent.get(p) : t;
        nodesToInfo.computeIfAbsent(reuseAt, k -> new ArrayList<>()).add(i);
        for (PlanNode reusedPlan : i.plans) {
          nodesToReuseNode.put(reusedPlan, i.builder.getNextReuseNode(reusedPlan.getArity()));
        }
      }
    });
    
    return t.transform((old, node, oldPath) -> {
      if (nodesToReuseNode.containsKey(old)) {
        return nodesToReuseNode.get(old);
      }
      List<Info> info = nodesToInfo.get(old);
      if (info != null) {
        PlanNode mat = node;
        for (Info i : info) {
          mat = i.builder.build(mat, i.leaf, i.plans.stream().map(p -> p.transform((subOld, subNode, subOldPath) -> {
            if (subOld.equals(p)) return subNode;
            PlanNode rn = nodesToReuseNode.get(subOld);
            return rn == null ? subNode : rn;
          })).collect(Collectors.toList()));
        }
        return mat;
      }
      return node;
    }, null);
  }

  /** Print debug information */
  @SuppressWarnings("unused")
  private void print(PlanNode root) {
    System.out.println("materialized plan");
    System.out.println(root.toPrettyString((node, str) -> {
      Info info = planToInfo.get(node);
      String add = "";
      if (info != null) {
        add = info.toString();
      }
      return String.format(" %-50s%s", add, str);
    }, placeholderToParent));
  }

  /**
   * Analyze the structure of the plan
   * @param p
   * @param depth depth of p
   */
  private PlanNode analyzeStructure(PlanNode p, int depth) {
    // TODO: multioutput for placeholders
    for (PlaceholderNode ph : p.placeholders()) {
      placeholderToParent.put(ph, p);
    }

    if (p.children().size() == 0 && !(p instanceof PlaceholderNode)) {
      planToInfo.computeIfAbsent(p, k -> new Info());
      return p;
    }

    if (p instanceof ProjectNode || p instanceof ConstantEqualityFilterNode || p instanceof VariableEqualityFilterNode) {
      PlanNode leaf = analyzeStructure(p.children().iterator().next(), depth + 1);
      if (leaf != null) {
        return leaf;
      }
    }
    else {
      for (PlanNode c : p.children()) {
        PlanNode leaf = analyzeStructure(c, depth + 1);
        if (leaf != null && leaf != c) {
          Info i = planToInfo.computeIfAbsent(leaf, k -> new Info());
          i.leaf = leaf;
          i.plans.add(c);
        }
      }
    }

    return null;
  }

  /** Statistics for reusing subplans */
  private class Info {

    PlanNode leaf = null;

    Set<PlanNode> plans = new HashSet<>();

    MultiOutputNode.Builder builder = new MultiOutputNode.Builder();

    boolean createMultiOutputNode = false;

    @Override
    public String toString() {
      return "";
    }
  }

}
