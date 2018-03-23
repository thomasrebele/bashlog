package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;

public class PlaceholderNode implements PlanNode {

  protected String operatorString;

  protected final Integer arity;

  public static class Builder {

    private PlaceholderNode node;

    public Builder(String operatorString, Integer arity) {
      node = new PlaceholderNode(operatorString, arity);
    }

    public PlaceholderNode preview() {
      return node;
    }

    public PlaceholderNode build() {
      if (node == null) throw new IllegalStateException("already built!");
      try {
        //node.operatorString = operatorString;
        return node;
      } finally {
        node = null;
      }
    }
  }

  public PlaceholderNode(String operatorString, Integer arity) {
    this.operatorString = operatorString;
    this.arity = arity;
    if (arity == null) {
      throw new NullPointerException();
    }
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
    return 0;
  }

  @Override
  public int getArity() {
    return arity == null ? -1 : arity;
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

}