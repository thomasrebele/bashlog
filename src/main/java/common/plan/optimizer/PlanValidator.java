package common.plan.optimizer;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.plan.node.*;

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
    Set<PlaceholderNode> allPlaceholders = new HashSet<>();
    Map<PlaceholderNode, Set<PlanNode>> placeholderToParent = new HashMap<>();

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
      else if (n instanceof PlaceholderNode) {
        check((PlaceholderNode) n);
      }

      allNodes.add(n);
      n.placeholders().forEach(p -> placeholderToParent.computeIfAbsent(p, k -> new HashSet<>(1)).add(n));
      if (n instanceof PlaceholderNode) allPlaceholders.add((PlaceholderNode) n);
      return n;
    });

    allPlaceholders.removeAll(placeholderToParent.keySet());
    if (allPlaceholders.size() > 0) {
      if (debug != null) {
        LOG.error("{}", debug);
      }
      allPlaceholders.stream() //
          .forEach(n -> LOG.error("parent of placeholder {} not found", n.operatorString()));
      throw new IllegalStateException("some orphaned placeholders, check log messages. " + (debug != null ? "debug length: " + debug.length() : ""));
    }

    Set<Entry<PlaceholderNode, Set<PlanNode>>> cuckoos = placeholderToParent.entrySet().stream()//
        .filter(e -> e.getValue().size() > 1)
        .collect(Collectors.toSet());

    if (cuckoos.size() > 0) {
      LOG.error("warning: some placeholders used multiple times: ");
      cuckoos.forEach(e -> LOG.error("  {} in {}", e.getKey(), e.getValue()));
      throw new IllegalStateException();
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

  private void check(PlaceholderNode n) {
    if (n.getArity() < 0) {
      LOG.error("placeholder has no arity: {}", n);
    }
  }

}
