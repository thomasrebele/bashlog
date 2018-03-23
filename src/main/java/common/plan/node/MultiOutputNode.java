
package common.plan.node;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiOutputNode implements PlanNode {

  private final PlanNode mainPlan;

  private final PlanNode leafPlan;

  private final List<PlanNode> reusedPlans;

  private final List<PlaceholderNode> reuseNodes;

  public static class Builder {

    List<PlaceholderNode> reuseNodes = new ArrayList<>();

    public PlanNode getNextReuseNode(int arity) {
      PlaceholderNode reuseNodeBuilder = new PlaceholderNode("mo_{building}", arity);
      reuseNodes.add(reuseNodeBuilder);
      return reuseNodeBuilder;
    }

    public MultiOutputNode build(PlanNode mainPlan, PlanNode leafPlan, List<PlanNode> reusedPlans) {
      if (reuseNodes == null) throw new IllegalStateException("already built!");
      MultiOutputNode result = new MultiOutputNode(mainPlan, leafPlan, reusedPlans, reuseNodes);
      return result;
    }
  }

  /** Use builder if possible */
  protected MultiOutputNode(PlanNode mainPlan, PlanNode leafPlan, List<PlanNode> reusedPlans, List<PlaceholderNode> reuseNodes) {
    // first initialize reuse node!
    if (!reuseNodes.stream().allMatch(p -> p instanceof PlaceholderNode)) {
      throw new IllegalArgumentException();
    }

    this.reusedPlans = reusedPlans;
    this.reuseNodes = reuseNodes;
    for (int i = 0; i < reusedPlans.size(); i++) {
      if (!mainPlan.contains(reuseNodes.get(i))) {
        System.err.println("exception while constructing " + this.operatorString());
        System.err.println("main plan:\n" + mainPlan.toPrettyString());
        throw new IllegalArgumentException(
            "incorrect reuse node specified:" + reuseNodes.get(i).toPrettyString() + "\nfor reuse plan\n" + reusedPlans.get(i).toPrettyString());
      }
    }
    this.mainPlan = mainPlan;
    this.leafPlan = leafPlan;
  }

  public PlanNode getMainPlan() {
    return mainPlan;
  }

  @Override
  public int getArity() {
    return mainPlan == null ? 0 : mainPlan.getArity();
  }

  @Override
  public String operatorString() {
    return "mo_" + hash();
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(leafPlan, mainPlan); //
  }

  @Override
  public List<PlaceholderNode> placeholders() {
    return reuseNodes;
  }

  @Override
  public List<PlanNode> childrenForPrettyString() {
    return Stream.concat(reusedPlans.stream(), Stream.of(mainPlan)).collect(Collectors.toList());
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newMainPlan = mainPlan.transform(fn, this);
    PlanNode newLeafPlan = leafPlan.transform(fn, this);
    List<PlanNode> newReusedPlans = reusedPlans.stream().map(n -> n.transform(fn, this)).collect(Collectors.toList());
    //List<PlanNode> newReusedPlans = reusedPlans;
    PlanNode newNode = this;
    if (!(mainPlan.equals(newMainPlan) && leafPlan.equals(newLeafPlan) && reusedPlans.equals(newReusedPlans))) {
      try {
        newNode = new MultiOutputNode(newMainPlan, newLeafPlan, newReusedPlans, reuseNodes);
      } catch (Exception e) {
        System.err.println("problem transforming multi output node\n" + this.toPrettyString());
        throw e;
      }
    }
    return fn.apply(this, newNode, originalParent);
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if (this == obj) return true;
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    MultiOutputNode node = (MultiOutputNode) obj;
    if (this.reusedPlans.size() != node.reusedPlans.size()) return false;

    // TODO: make check independent of reuse plan order
    Map<PlanNode, PlanNode> newEqualities = new HashMap<>(assumedEqualities);
    for (int i = 0; i < node.reusedPlans.size(); i++) {
      newEqualities.put(this.reuseNodes.get(i), node.reuseNodes.get(i));
    }

    if (!leafPlan.equals(node.leafPlan)) return false;
    if (!mainPlan.equals(node.mainPlan, assumedEqualities)) return false;
    for (int i = 0; i < node.reusedPlans.size(); i++) {
      if (!this.reusedPlans.get(i).equals(node.reusedPlans.get(i), newEqualities)) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mainPlan, leafPlan);
  }

  public List<PlanNode> reusedPlans() {
    return Collections.unmodifiableList(reusedPlans);
  }

  public List<PlanNode> reuseNodes() {
    return Collections.unmodifiableList(reuseNodes);
  }

  public PlanNode getLeaf() {
    return leafPlan;
  }
}
