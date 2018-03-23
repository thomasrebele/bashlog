package common.plan.optimizer;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.plan.node.PlaceholderNode;
import common.plan.node.PlanNode;
import common.plan.node.UnionNode;

/**
 * Checks a plan for errors
 * - union nodes having a union node as child
 * - plans that contain placeholders, but no parents
 */
public class PlanValidator implements Optimizer {

  private static final Logger LOG = LoggerFactory.getLogger(PlanValidator.class);

  private CharSequence debug = null;

  private static final String save = "mat_1777";

  Set<PlanNode> saved = new HashSet<>();

  public PlanValidator(CharSequence debugInfo) {
    this.debug = debugInfo;
  }

  @Override
  public PlanNode apply(PlanNode node) {
    Map<PlanNode, Set<PlanNode>> parentToPlaceholders = new HashMap<>();
    Set<PlanNode> allNodes = new HashSet<>();

    node.transform((orig, n, origParent) -> {
      if (save != null) {
        if (n.operatorString().contains(save)) {
          saved.add(n);
          if (saved.size() > 1) {
            System.out.println("here");
          }
        }
      }
      if (n instanceof UnionNode) {
        check((UnionNode) n);
      }

      allNodes.add(n);
      if (n.placeholders().size() > 0) {
        parentToPlaceholders.put(n, new HashSet<>(n.placeholders()));
      }
      return n;
    });

    parentToPlaceholders.keySet().removeAll(allNodes);
    if (parentToPlaceholders.size() > 0) {
      if (debug != null) {
        LOG.error("{}", debug);
      }
      parentToPlaceholders.values().stream().flatMap(s -> s.stream())
          .forEach(n -> LOG.error("parent of placeholder {} not found", n.operatorString()));
      throw new IllegalStateException("some orphaned placeholders, check log messages. " + (debug != null ? "debug length: " + debug.length() : ""));
    }

    return node;
  }

  private void check(UnionNode n) {
    for (PlanNode c : n.children()) {
      if (c instanceof UnionNode) {
        LOG.error("union may not have a union as child");
      }
    }
  }

}
