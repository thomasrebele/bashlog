
package common.plan.node;

import common.Tools;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiOutputNode implements PlanNode {

  private final PlanNode mainPlan;

  private final PlanNode leafPlan;

  private final List<PlanNode> reusedPlans;

  private final List<PlanNode> reuseNodes;

  public static class Builder {

    List<PlaceholderNode.Builder> reuseNodeBuilders = new ArrayList<>();

    public PlanNode getNextReuseNode(int arity) {
      PlaceholderNode.Builder reuseNodeBuilder = new PlaceholderNode.Builder("mo_{building}", arity);
      reuseNodeBuilders.add(reuseNodeBuilder);
      return reuseNodeBuilder.preview();
    }

    public MultiOutputNode build(PlanNode mainPlan, PlanNode leafPlan, List<PlanNode> reusedPlans) {
      if (reuseNodeBuilders == null) throw new IllegalStateException("already built!");
      MultiOutputNode result = new MultiOutputNode(mainPlan, leafPlan, reusedPlans, reuseNodeBuilders, null);
      for (int i = 0; i < reusedPlans.size(); i++) {
        String op = result.operatorString() + " reuse " + i + " (" + result.reusedPlans.get(i).hash() + ")";
        reuseNodeBuilders.get(i).build(result, op);
      }
      return result;
    }
  }

  /** Use builder if possible */
  protected MultiOutputNode(PlanNode mainPlan, PlanNode leafPlan, List<PlanNode> reusedPlans, List<PlaceholderNode.Builder> builders,
      List<PlanNode> reuseNodes) {
    // first initialize reuse node!
    if (builders != null) {
      reuseNodes = new ArrayList<>(builders.size());
      for (PlaceholderNode.Builder builder : builders) {
        builder.setParent(this);
        reuseNodes.add(builder.preview());
      }
    }

    if (!reuseNodes.stream().allMatch(p -> p instanceof PlaceholderNode)) {
      throw new IllegalArgumentException();
    }

    this.reusedPlans = reusedPlans;
    if (builders != null) {
      this.reuseNodes = reuseNodes;
    } else {
      this.reuseNodes = new ArrayList<>();
      for (int i = 0; i < reusedPlans.size(); i++) {
        PlanNode oldReuseNode = reuseNodes.get(i);
        PlanNode newReuseNode = new PlaceholderNode(this, oldReuseNode.operatorString(), reusedPlans.get(i).getArity());
        this.reuseNodes.add(newReuseNode);

        if (!mainPlan.contains(oldReuseNode)) {
          throw new IllegalArgumentException("incorrect reuse node specified:" + oldReuseNode.toPrettyString() + "\n" + mainPlan.toPrettyString());
        }
        mainPlan = mainPlan.replace(oldReuseNode, newReuseNode);
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
    return Arrays.asList(leafPlan, mainPlan); //Stream.concat(reusedPlans.stream(), Stream.of(mainPlan)).collect(Collectors.toList());
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newMainPlan = mainPlan.transform(fn, this);
    PlanNode newLeafPlan = leafPlan.transform(fn, this);
    List<PlanNode> newReusedPlans = reusedPlans.stream().map(n -> n.transform(fn, this)).collect(Collectors.toList());
    PlanNode newNode = mainPlan.equals(newMainPlan) && leafPlan.equals(newLeafPlan) && reusedPlans.equals(newReusedPlans) ? this
        : new MultiOutputNode(newMainPlan, newLeafPlan, newReusedPlans, null, reuseNodes);
    return fn.apply(this, newNode, originalParent);
  }

  @Override
  public boolean equals(Object obj) {
    return equals(obj, Collections.emptyMap());
  }

  @Override
  public boolean equals(Object obj, Map<PlanNode, PlanNode> assumedEqualities) {
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    MultiOutputNode node = (MultiOutputNode) obj;
    if(this.reusedPlans.size() != node.reusedPlans.size()) return false;
    
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
    return mainPlan.getClass().hashCode() ^ mainPlan.getArity() ^ leafPlan.getClass().hashCode() ^ leafPlan.getArity();
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
