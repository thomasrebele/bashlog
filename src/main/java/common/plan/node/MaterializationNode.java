package common.plan.node;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.Tools;

public class MaterializationNode implements PlanNode {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializationNode.class);

  private final PlanNode mainPlan;

  private final PlanNode reusedPlan;

  private final PlaceholderNode reuseNode;

  /** see {@link #getReuseCount()} */
  private final int reuseCount;

  public static class Builder {

    PlaceholderNode placeholder;

    public Builder(int arity) {
      placeholder = new PlaceholderNode("mat_{building}", arity);//new ReuseNode(null, arity);
    }

    public PlanNode getReuseNode() {
      return placeholder;
    }

    public MaterializationNode build(PlanNode mainPlan, PlanNode reusedPlan, int reuseCount) {
      MaterializationNode result = new MaterializationNode(mainPlan, reusedPlan, placeholder, reuseCount);
      return result;
    }

  }

  /** Use builder if possible */
  protected MaterializationNode(PlanNode mainPlan, PlanNode reusedPlan, PlaceholderNode reuseNode, int reuseCount) {
    // first initialize reuse node!

    if (!(reuseNode instanceof PlaceholderNode)) {
      throw new IllegalArgumentException();
    }
    this.reuseNode = reuseNode;
    this.reusedPlan = reusedPlan;
    if (!mainPlan.contains(reuseNode)) {
      System.err.println("exception while constructing " + this.operatorString());
      System.err.println("main plan:\n" + mainPlan.toPrettyString());
      throw new IllegalArgumentException(
          "incorrect reuse node specified:" + reuseNode.toPrettyString() + "\nfor reuse plan\n" + reusedPlan.toPrettyString());
    }
    this.mainPlan = mainPlan.replace(reuseNode, this.reuseNode);
    this.reuseCount = reuseCount;
  }

  public PlanNode getReusedPlan() {
    return reusedPlan;
  }

  public PlanNode getMainPlan() {
    return mainPlan;
  }

  public PlaceholderNode getReuseNode() {
    return reuseNode;
  }

  /** How often the plan is reused. Infinity is denoted by -1 */
  public int getReuseCount() {
    return reuseCount;
  }

  @Override
  public int getArity() {
    return mainPlan == null ? 0 : mainPlan.getArity();
  }

  @Override
  public String operatorString() {
    return "mat_" + hash();
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(reusedPlan, mainPlan);
  }

  @Override
  public List<PlaceholderNode> placeholders() {
    return Arrays.asList(reuseNode);
  }

  @Override
  public PlanNode transform(TransformFn fn, List<PlanNode> originalPath) {
    try {
      Tools.addLast(originalPath, this);
      PlanNode newMainPlan = mainPlan.transform(fn, originalPath);
      PlanNode newReusedPlan = reusedPlan.transform(fn, originalPath);
      PlanNode newNode = mainPlan.equals(newMainPlan) && reusedPlan.equals(newReusedPlan) ? this
          : new MaterializationNode(newMainPlan, newReusedPlan, reuseNode, reuseCount);
      return fn.apply(this, newNode, originalPath);
    } finally {
      Tools.removeLast(originalPath);
    }
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
    MaterializationNode node = (MaterializationNode) obj;
    assumedEqualities = Tools.with(assumedEqualities, reuseNode, node.reuseNode);
    return mainPlan.equals(node.mainPlan, assumedEqualities) && reusedPlan.equals(node.reusedPlan, assumedEqualities);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mainPlan, reusedPlan);
  }

  @Override
  public String toString() {
    return operatorString();
  }
}
