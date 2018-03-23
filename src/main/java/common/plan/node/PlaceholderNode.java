package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;

public class PlaceholderNode implements PlanNode {

  protected PlanNode parent;

  protected String operatorString;

  protected final Integer arity;

  public static class Builder {

    private PlaceholderNode node;

    public Builder(String operatorString, Integer arity) {
      node = new PlaceholderNode(null, operatorString, arity);
    }

    public PlaceholderNode preview() {
      return node;
    }

    public void setParent(PlanNode parent) {
      node.parent = parent;
    }

    /** Only use for debugging */
    public void setOperatorString(String str) {
      node.operatorString = str;
    }

    public PlaceholderNode build(PlanNode parent, String operatorString) {
      if (node == null) throw new IllegalStateException("already built!");
      try {
        node.parent = parent;
        node.operatorString = operatorString;
        return node;
      } finally {
        node = null;
      }
    }
  }

  public PlaceholderNode(PlanNode parent, String operatorString) {
    this.parent = parent;
    this.operatorString = operatorString;
    arity = null;
  }

  public PlaceholderNode(PlanNode parent, String operatorString, Integer arity) {
    this.parent = parent;
    this.operatorString = operatorString;
    this.arity = arity;
  }

  /*public PlanNode getParent() {
    return parent;
  }*/

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
    return arity == null ? parent.getArity() : arity;
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public String operatorString() {
    return operatorString + (parent != null && !operatorString.contains(" for ") ? " for " + parent.operatorString() : "");
  }

  /** Walk through the plan and collect outer nodes that have a place holder in the plan given by argument 'ofPlan' */
  /*public static Set<PlanNode> outerParents(PlanNode ofPlan) {
    HashMap<PlanNode, Boolean> nodeToContained = new HashMap<>();
  
    ofPlan.transform(pn -> {
      nodeToContained.put(pn, true);
  
      if (pn instanceof PlaceholderNode) {
        nodeToContained.putIfAbsent(((PlaceholderNode) pn).getParent(), false);
      }
      return pn;
    });
  
    return nodeToContained.entrySet().stream().filter(e -> !e.getValue()).map(e -> e.getKey()).collect(Collectors.toSet());
  }*/

}