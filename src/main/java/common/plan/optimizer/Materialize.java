package common.plan.optimizer;

import java.util.*;

import bashlog.plan.TSVFileNode;
import common.plan.node.*;

/**
 * Create materialization nodes in a plan, in order to reuse common subplans.
 * A subplan should be reused if it is used multiple times, and doesn't change between reuses.
 */
public class Materialize implements Optimizer {

  private HashMap<PlanNode, Info> planToInfo;

  @Override
  public PlanNode apply(PlanNode t) {
    planToInfo = new HashMap<>();
    analyzeStructure(t, 0, new ArrayList<>(), new HashSet<>(), new HashSet<>());
    analyzeReuse(t, 1);
    analyzeMaterialize(t, false);
    //print(t);

    HashMap<PlanNode, List<Info>> nodesToInfo = new HashMap<>();
    HashMap<PlanNode, PlanNode> nodesToReuseNode = new HashMap<>();
    planToInfo.forEach((p, i) -> {
      if (i.reuse()) {
        PlanNode reuseAt = i.reuseAt() == null ? t : i.reuseAt();
        nodesToInfo.computeIfAbsent(reuseAt, k -> new ArrayList<>()).add(i);
        nodesToReuseNode.put(i.plan, i.matNodeBuilder.getReuseNode());
      }
    });

    return t.transform((old, node, parent) -> {
      if (nodesToReuseNode.containsKey(old)) {
        return nodesToReuseNode.get(old);
      }
      List<Info> info = nodesToInfo.get(old);
      if (info != null) {
        info.get(0).plan.height();
        Comparator<Info> cmp = Comparator.comparing((i) -> -i.plan.height());
        cmp.thenComparing(new Comparator<Info>() {

          @Override
          public int compare(Info i1, Info i2) {
            if (i1.plan.contains(i2.plan)) return -1;
            if (i2.plan.contains(i1.plan)) return 1;
            return 0;
          }
        })
            .thenComparingInt(i -> i.depth) //
            .thenComparingInt(i -> System.identityHashCode(i));

        info.sort(cmp);

        PlanNode mat = node;
        for (Info i : info) {
          PlanNode newReusedPlan = i.plan.transform((o, pn, p) -> {
            // compare with 'old' node, as plan tree might have changed due to preceding materializations
            if (o.equals(i.plan)) return pn;
            PlanNode rn = nodesToReuseNode.get(o);
            return rn == null ? pn : rn;
          });
          mat = i.matNodeBuilder.build(mat, newReusedPlan, i.useCount());
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
    }));
  }

  /**
   * Analyze the structure of the plan
   * @param p
   * @param depth depth of p
   * @param outerRecursions recursions that contain p
   * @param innerRecursions will contain recursions contained in p (at the end of the function call)
   * @param calledRecursions will contain recursions whose delta/full nodes are contained in p (at the end of the function call)
   */
  private void analyzeStructure(PlanNode p, int depth, List<PlanNode> outerRecursions, //
      Set<PlanNode> innerRecursions, Set<PlanNode> calledRecursions) {

    if (p instanceof PlaceholderNode && ((PlaceholderNode) p).getParent() instanceof RecursionNode) {
      calledRecursions.add(((PlaceholderNode) p).getParent());
      return;
    }

    for (PlanNode child : p.children()) {
      if (p instanceof RecursionNode && child == ((RecursionNode) p).getRecursivePlan()) {
        outerRecursions.add(p);
      }
      HashSet<PlanNode> tmpRecCalls = new HashSet<>(), tmpInnerRec = new HashSet<>();
      analyzeStructure(child, depth + 1, outerRecursions, tmpInnerRec, tmpRecCalls);
      calledRecursions.addAll(tmpRecCalls);
      innerRecursions.addAll(tmpInnerRec);
    }
    if (p instanceof RecursionNode) {
      outerRecursions.remove(outerRecursions.size() - 1);
      calledRecursions.remove(p);
    }

    Info info = planToInfo.computeIfAbsent(p, k -> new Info(p, innerRecursions, calledRecursions));
    info.outerRecursions.addAll(outerRecursions);
    info.depth = depth;
    if (info.filename == null) info.filename = "tmp/relation" + planToInfo.size();
    info.planUseCount++;

  }

  /**
   * Check whether subtree is used more often than their parent.
   * Plans containing delta nodes cannot be reused.
   */
  private void analyzeReuse(PlanNode p, int parentUseCount) {
    Info info = planToInfo.get(p);
    if (info == null) return;
    if (p instanceof PlaceholderNode) return;

    if (info.planUseCount > parentUseCount) {
      info.reuse = true;
      parentUseCount = info.planUseCount;
    }
    int fUseCount = parentUseCount;
    p.children().forEach(c -> analyzeReuse(c, fUseCount));
  }

  /**
   * Determine at which point materialization should occur
   */
  private void analyzeMaterialize(PlanNode p, boolean parentIsRepeated) {
    Info info = planToInfo.get(p);
    if (info != null) {
      if (info.isRepeated() && !parentIsRepeated) {
        info.reuse = true;
      }
      p.children().forEach(c -> analyzeMaterialize(c, info.isRepeated()));
    }
  }

  /** Statistics for reusing subplans */
  private class Info {

    PlanNode plan;

    /** Depth within execution plan */
    int depth;

    /** Materialized table / prefix for named pipes (filename_0, filename_1, ...) */
    String filename;

    /** Count how often plan node appears in tree */
    int planUseCount = 0;

    /** Whether this plan is repeated in a recursion node. Calculated by isRepeated() */
    Boolean isRepeated = null;

    /** Recursion ancestor nodes, which iterate over this plan. Used for check whether to materialize this plan. */
    final Set<PlanNode> outerRecursions = new HashSet<>();

    /**
     * Outer recursions whose delta/full node is contained in this plan.
     */
    final Set<PlanNode> calledRecursions = new HashSet<>();

    /** If reuse, either materialize output to file, or create named pipes and fill them with tee */
    private boolean reuse = false;

    /** Materialize below this node */
    PlanNode materializeAt = null;

    /** If this plan is materialized, use this builder to obtain a reuse node before creating the materialization node */
    MaterializationNode.Builder matNodeBuilder;

    public Info(PlanNode plan, Set<PlanNode> innerRecursions, Set<PlanNode> calledRecursions) {
      this.plan = plan;
      matNodeBuilder = new MaterializationNode.Builder(plan.getArity());
      this.calledRecursions.addAll(calledRecursions);
      this.calledRecursions.removeAll(innerRecursions);
    }

    /** How often the plan is used during execution; -1 represents infinity */
    public int useCount() {
      if (isRepeated() == true) return -1;
      return planUseCount;
    }

    boolean reuse() {
      return reuse && !(plan instanceof BuiltinNode || plan instanceof TSVFileNode);
    }

    /** Get node with the deepest depth, and store depth in 2nd parameter */
    PlanNode maxDepth(Set<PlanNode> nodes, int[] depth) {
      PlanNode result = null;
      for (PlanNode n : nodes) {
        int nDepth = Materialize.this.planToInfo.get(n).depth;
        if (nDepth > depth[0]) {
          depth[0] = nDepth;
          result = n;
        }
      }
      return result;
    }

    /** Calculate which node should contain the materialization node for this plan (if at all) */
    PlanNode reuseAt() {
      if (isRepeated == null) {
        int[] calledDepth = new int[] { -1 }, outerDepth = new int[] { -1 };
        materializeAt = maxDepth(calledRecursions, calledDepth);
        maxDepth(outerRecursions, outerDepth);
        // check whether this plan is contained within a recursion node, but doesn't contain its delta/full nodes
        // in that case we should materialize it at node 'materializeAt'
        isRepeated = outerDepth[0] > calledDepth[0];
      }
      return materializeAt;
    }

    /** Check whether this plan is contained within a recursion node, but doesn't contain its delta/full nodes.
     * This calculated at reuseAt(...) */
    boolean isRepeated() {
      if (isRepeated == null) {
        reuseAt();
      }
      return isRepeated;
    }

    @Override
    public String toString() {
      return //filename + " uc:" + planUseCount + //
          " orc:" + outerRecursions.size() + //
          " crc:" + calledRecursions.size() + //
          " " + (reuse() ? "reuse" : "     ") + //
          " " + (useCount() < 0 ? "-" : useCount()) + "x " + //
          (reuseAt() != null ? " at " + reuseAt().hash() : "") + "  " + plan.toString();
    }
  }

}
