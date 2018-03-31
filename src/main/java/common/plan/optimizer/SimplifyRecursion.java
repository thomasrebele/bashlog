package common.plan.optimizer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import common.Tools;
import common.plan.node.*;

/**
 * Simplify recursions, and remove unnecessary ones
 */
public class SimplifyRecursion implements Optimizer {

  private static final Logger LOG = LoggerFactory.getLogger(SimplifyRecursion.class);

  Map<PlaceholderNode, PlanNode> placeholderToParent = null;

  public PlanNode apply(PlanNode node) {
    placeholderToParent = PlaceholderNode.placeholderToParentMap(node);
    return node.transform((n) -> {
      if (n instanceof RecursionNode) {
        return simplify((RecursionNode) n);
      } else if (n instanceof JoinNode) {
        // TODO: ...
        return n;
      } else {
        return n;
      }
    });
  }

  private PlanNode simplify(RecursionNode node) {
    PlanNode exitPlan = node.getExitPlan();
    PlanNode recursivePlan = node.getRecursivePlan();

    recursivePlan = removeUnnecessaryPlaceholders(recursivePlan, node);
    
    {
      PlanNode newRecursivePlan = PlanNode.empty(node.getArity());
      // we see if the recursion is a union with some not recursive parts and move them to exitPlan
      for (PlanNode child : (recursivePlan instanceof UnionNode) ? recursivePlan.children() : Collections.singleton(recursivePlan)) {
        if (child.contains(node.getFull()) || child.contains(node.getDelta())) {
          newRecursivePlan = newRecursivePlan.union(child);
        } else {
          exitPlan = exitPlan.union(child);
        }
      }
      recursivePlan = newRecursivePlan;
    }
    recursivePlan = introduceDeltaRecursion(recursivePlan, node.getDelta(), node.getFull());


    // we make sure that exit and recursive plans are simplified
    exitPlan = apply(exitPlan);
    PlanNode newRecursivePlan = apply(recursivePlan);

    // if a recursion was optimized away, we might be able to remove more delta/full nodes
    recursivePlan = newRecursivePlan == recursivePlan ? recursivePlan : removeUnnecessaryPlaceholders(newRecursivePlan, node);

    // recursion was optimized away
    if (recursivePlan.isEmpty()) {
      return exitPlan;
    }

    // optimization for transitive closure
    PlanNode transClosure1 = (node.getDelta().join(node.getFull(), new int[] { 1 }, new int[] { 0 }))
        .union(node.getFull().join(node.getDelta(), new int[] { 1 }, new int[] { 0 })).project(new int[] { 0, 3 });

    PlanNode transClosure2 = (node.getDelta().join(node.getFull(), new int[] { 0 }, new int[] { 1 }))
        .union(node.getFull().join(node.getDelta(), new int[] { 0 }, new int[] { 1 })).project(new int[] { 1, 2 });

    if (recursivePlan.equals(transClosure1) || recursivePlan.equals(transClosure2)) {
      recursivePlan = node.getDelta().join(node.getFull(), new int[] { 1 }, new int[] { 0 }).project(new int[] { 0, 3 });
    }

    // something changed, create new recursion node
    if (!Objects.equals(exitPlan, node.getExitPlan()) || !Objects.equals(recursivePlan, node.getRecursivePlan())) {
      return new RecursionNode(exitPlan, recursivePlan, node.getDelta(), node.getFull());
    }
    return node;
  }

  private PlanNode introduceDeltaRecursion(PlanNode baseNode, PlanNode delta, PlanNode full) {
    return baseNode.transform(n -> {
      //We look for joins between to subtrees depending on dela and we replace one by Full
      if (n == full) {
        return delta;
      }

      if (n instanceof JoinNode) {
        JoinNode join = (JoinNode) n;
        if (join.getLeft().contains(delta) && join.getRight().contains(delta)) {
          return join.getLeft().join(join.getRight().replace(delta, full), join.getLeftProjection(), join.getRightProjection())
              .union(join.getLeft().replace(delta, full).join(join.getRight(), join.getLeftProjection(), join.getRightProjection()));
        }
      }
      return n;
    });
  }

  /**
   * Get all paths that end in placeholders
   * @param recursivePlan
   * @return set of paths to placeholder
   */
  private Set<List<PlanNode>> placeholders(PlanNode recursivePlan, RecursionNode rec) {
    Set<List<PlanNode>> result = new HashSet<>();

    recursivePlan.transform((o, n, path) -> {
      if (n.equals(rec.getDelta()) || n.equals(rec.getFull())) {
        result.add(new ArrayList<>(path));
      }
      return n;
    }, new ArrayList<>());

    return result;
  }

  /**
   * Remove unnecessary placeholders
   * Such a placeholder is unnecessary, if (and only if) all its columns arrive at the output, in that order
   * I.e., that placeholder gets just copied to the output, does not produce any new rows
   * @param recursivePlan
   * @param rec
   * @return recursive plan without unnecessary placeholders
   */
  private PlanNode removeUnnecessaryPlaceholders(PlanNode recursivePlan, RecursionNode rec) {
    // search for unnecessary delta/full nodes
    Set<List<PlanNode>> placeholders = placeholders(recursivePlan, rec);
    Set<List<PlanNode>> removable = placeholders.stream().filter(this::isUnnecessary).collect(Collectors.toSet());

    // remove unnecessary nodes
    recursivePlan = recursivePlan.transform((o, n, path) -> {
      if (removable.contains(path)) {
        return PlanNode.empty(n.getArity());
      }
      return n;
    }, new ArrayList<>());
    return recursivePlan;
  }

  /**
   * Check whether a placeholder is unnecessary
   * @param path
   * @return true if it can be removed
   */
  private boolean isUnnecessary(List<PlanNode> path) {
    int arity = Tools.index(path, -1).getArity();
    List<Integer> cols = IntStream.range(0, arity).mapToObj(i -> i).collect(Collectors.toCollection(() -> new ArrayList<>()));
    List<Integer> orig = new ArrayList<>(cols);

    for (int i = path.size() - 2; i >= 0; i--) {
      PlanNode n = path.get(i);
      if (n instanceof RecursionNode) {
        return false;
      } else if (n instanceof EqualityFilterNode || n instanceof UnionNode) {
        // do nothing
      } else if (n instanceof ProjectNode) {
        ProjectNode p = (ProjectNode) n;
        List<Integer> fcols = cols;
        cols = Arrays.stream(p.getProjection()).mapToObj(c -> c < 0 ? -1 : fcols.get(c)).collect(Collectors.toList());

      } else if (n instanceof JoinNode) {
        JoinNode j = (JoinNode) n;
        PlanNode child = path.get(i + 1);
        if (j.getLeft() == child) {
          cols.addAll(IntStream.range(0, j.getRight().getArity()).mapToObj(x -> -1).collect(Collectors.toList()));
        } else {
          List<Integer> newCols = IntStream.range(0, j.getLeft().getArity()).mapToObj(x -> -1)
              .collect(Collectors.toCollection(() -> new ArrayList<>()));
          newCols.addAll(cols);
          cols = newCols;
        }
      } else {
        LOG.warn("don't know what to do with {}", n.getClass());
        return false;
      }
    }

    LOG.trace("cols {} for {}", cols, PlanNode.pathToString(path));
    return cols.equals(orig);
  }

  // TODO: move to test
  public static void main(String[] args) {
    RecursionNode.Builder builder = new RecursionNode.Builder(2);
    builder.addRecursivePlan(builder.getDelta());
    /*builder.addRecursivePlan(builder.getDelta().union(builder.getFull()));
    builder.addRecursivePlan(builder.getDelta().join(builder.getFull(), new int[] { 1 }, new int[] { 0 }).project(new int[] { 0, 3 }));
    builder.addRecursivePlan(builder.getDelta().join(builder.getFull(), new int[] { 1 }, new int[] { 0 }).project(new int[] { 0, 1 }));
    builder.addRecursivePlan(builder.getDelta().project(new int[] { 1, 0 }));*/
    PlanNode p = builder.build(new FactNode("abc", "def"));

    System.out.println(p.toPrettyString());
    System.out.println("---");
    p = new SimplifyRecursion().apply(p);
    System.out.println("---\n");
    System.out.println(p.toPrettyString());
  }

}
