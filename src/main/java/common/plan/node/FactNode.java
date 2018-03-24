package common.plan.node;

import java.util.*;
public class FactNode implements PlanNode {

  private final List<Comparable<?>[]> facts;

  public FactNode(Comparable<?>[] fields) {
    facts = new ArrayList<>();
    facts.add(fields);
  }

  public FactNode(List<FactNode> list) {
    facts = new ArrayList<>();
    for (FactNode fn : list) {
      facts.addAll(fn.facts);
    }
    // TODO: check arity
  }

  public List<Comparable<?>[]> getFacts() {
    return facts;
  }

  @Override
  public int getArity() {
    return facts.get(0).length;
  }

  @Override
  public String operatorString() {
    if (facts.size() == 1) {
      return "fact " + Arrays.toString(facts.get(0));
    }
    return "facts (" + facts.size() + "x)";
  }

  @Override
  public List<PlanNode> children() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return facts.stream().mapToInt(f -> f.hashCode()).reduce(0, (a, b) -> a ^ b);
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if(this == obj) return true;
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    
    FactNode node = (FactNode) obj;
    return facts.equals(node.facts);
  }

}
