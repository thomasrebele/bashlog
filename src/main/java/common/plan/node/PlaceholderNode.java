package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;

public class PlaceholderNode implements PlanNode {

  protected String operatorString;

  protected final int arity;

  public PlaceholderNode(String operatorString, int arity) {
    this.operatorString = operatorString;
    this.arity = arity;
  }

  @Override
  public String toString() {
    return operatorString;
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj,  Map<PlanNode,PlanNode> assumedEqualities) {
    if (this == obj) return true;
    return assumedEqualities.getOrDefault(this, this) == obj;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public int getArity() {
    return arity;
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public String operatorString() {
    return operatorString;
  }

  /** Walk through plan, and create a map for obtaining the parents of placeholder nodes */
  public static Map<PlaceholderNode, PlanNode> placeholderToParentMap(PlanNode plan) {
    Map<PlaceholderNode, PlanNode> map = new HashMap<>();
    if (plan != null) {
      plan.transform(n -> {
        for (PlaceholderNode pn : n.placeholders()) {
          map.put(pn, n);
        }
        return n;
      });
    }
    return map;
  }

  /** Walk through the plan and collect outer nodes that have a place holder in the plan given by argument 'ofPlan' 
   * @param placeholderToParent */
  public static Set<PlanNode> outerParents(PlanNode ofPlan, Map<PlaceholderNode, PlanNode> placeholderToParent) {
    HashMap<PlanNode, Boolean> nodeToContained = new HashMap<>();
  
    ofPlan.transform(pn -> {
      nodeToContained.put(pn, true);
  
      if (pn instanceof PlaceholderNode) {
        nodeToContained.putIfAbsent(placeholderToParent.get(pn), false);
      }
      return pn;
    });
  
    return nodeToContained.entrySet().stream().filter(e -> !e.getValue()).map(e -> e.getKey()).collect(Collectors.toSet());
  }

  /** Get all placeholders that occur in a plan */
  public static Set<PlaceholderNode> searchInPlan(PlanNode plan) {
    Set<PlaceholderNode> placeholders = new HashSet<>();
    plan.transform(x -> {
      if (x instanceof PlaceholderNode) {
        placeholders.add((PlaceholderNode) x);
      }
      return x;
    });
    return placeholders;
  }

}