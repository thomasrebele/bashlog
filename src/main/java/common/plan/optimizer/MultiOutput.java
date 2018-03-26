package common.plan.optimizer;

import java.util.*;
import java.util.Map.Entry;
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
    // determine for which leaf nodes shall become a multi output
    planToInfo = new HashMap<>();
    analyzeStructure(t, 0);

    // inner node to placeholder
    HashMap<PlanNode, PlanNode> nodesToReuseNode = new HashMap<>();
    HashMap<PlanNode, Set<PlanNode>> reuseNodeToPlans = new HashMap<>();

    // where to put the multi output node (and if we need it at all)
    HashMap<PlanNode, List<Info>> nodesToInfo = new HashMap<>();
    planToInfo.forEach((p, i) -> {
      if (i.plansToParents.size() > 1) {
        PlanNode reuseAt = p instanceof PlaceholderNode ? placeholderToParent.get(p) : t;

        // make placeholders for subplans
        nodesToInfo.computeIfAbsent(reuseAt, k -> new ArrayList<>()).add(i);

        // group reuse nodes that appear within a the same unions
        Map<Set<PlanNode>, Set<PlanNode>> parentsToPlans = new HashMap<>();
        for (Entry<PlanNode, Set<PlanNode>> e : i.plansToParents.entrySet()) {
          parentsToPlans.computeIfAbsent(e.getValue(), k -> new HashSet<>()).add(e.getKey());
        }

        // every such group gets one reuse node
        for (Entry<Set<PlanNode>, Set<PlanNode>> e : parentsToPlans.entrySet()) {
          int arity = e.getValue().iterator().next().getArity();
          PlanNode reuseNode = i.builder.getNextReuseNode(arity);

          e.getValue().stream().forEach(reusedPlan -> {
            nodesToReuseNode.put(reusedPlan, reuseNode);
          });
          reuseNodeToPlans.put(reuseNode, e.getValue());
        }
      }
    });

    // create multi output nodes in the plan
    return t.transform((old, node, oldPath) -> {
      if (nodesToReuseNode.containsKey(old)) {
        return nodesToReuseNode.get(old);
      }
      // for all multi nodes that should be introduced at this node
      List<Info> info = nodesToInfo.get(old);
      if (info != null) {
        PlanNode mat = node;
        for (Info i : info) {
          List<PlanNode> reusedPlans = i.builder.getReuseNodes().stream().map(p -> {
            Set<PlanNode> replacedNodes = reuseNodeToPlans.get(p);
            PlanNode result = replacedNodes.size() == 1 ? replacedNodes.iterator().next() : new UnionNode(replacedNodes);

            // TODO: is this actually needed?
            result = result.transform((subOld, subNode, subOldPath) -> {
              if (replacedNodes.contains(subOld)) return subNode;
              PlanNode rn = nodesToReuseNode.get(subOld);
              return rn == null ? subNode : rn;
            });
            return result;
          }).collect(Collectors.toList());

          mat = i.builder.build(mat, i.leaf, reusedPlans);
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
    } else {
      for (PlanNode c : p.children()) {
        PlanNode leaf = analyzeStructure(c, depth + 1);
        if (leaf != null && leaf != c) {
          Info i = planToInfo.computeIfAbsent(leaf, k -> new Info());
          i.leaf = leaf;
          i.plansToParents.computeIfAbsent(c, k -> new HashSet<>()).add(p);
        }
      }
    }

    return null;
  }

  /** Statistics for reusing subplans */
  private class Info {

    /** A plan that occurs several times with different projections/selections */
    PlanNode leaf = null;

    /** The plans where the leaf occurs */
    Map<PlanNode, Set<PlanNode>> plansToParents = new HashMap<>();

    MultiOutputNode.Builder builder = new MultiOutputNode.Builder();
  }

}
