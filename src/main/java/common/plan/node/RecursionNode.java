package common.plan.node;

import common.Tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 
 * Represents a plan node, that has itself as subplan.
 * 
 * Usecase example:
 * r1: foo(x,y) <- bar(x,y)
 * r2: foo(x,z) <- foo(x,y), bar(y,z)
 * <p>
 * We will have for exit plan the plan for r1 (input of the loop) and we will add the recursive plan for r2 where
 * foo call is replaced by the delta node (retrieved by getDelta()) 
 */
public class RecursionNode implements PlanNode {

  private final PlanNode exitPlan;

  private PlanNode recursivePlan;

  protected final PlaceholderNode deltaNode;

  protected final PlaceholderNode fullNode;

  public static class Builder {

    int arity;

    PlaceholderNode delta, full;

    PlanNode recursivePlan;

    public Builder(int arity) {
      this.arity = arity;
      delta = new PlaceholderNode("delta", arity);
      full = new PlaceholderNode("full", arity);
      recursivePlan = PlanNode.empty(arity);
    }

    public RecursionNode build(PlanNode exitPlan) {
      RecursionNode r = new RecursionNode(exitPlan, recursivePlan, Builder.this);
      return r;
    }

    public PlaceholderNode getDelta() {
      return delta;
    }

    public PlaceholderNode getFull() {
      return full;
    }

    public void addRecursivePlan(PlanNode addedRecursivePlan) {
      if (addedRecursivePlan.getArity() != arity) {
        throw new IllegalArgumentException("Wrong arity for recursive plan.\nRecursion plan:\n" + addedRecursivePlan + "\nexpected arity:\n" + arity);
      }
      recursivePlan = recursivePlan.union(addedRecursivePlan);
    }
  }

  /** Use exit plan and recursive plan. The delta is the one in the recursive plan. It will be replaced by a new delta. */
  private RecursionNode(PlanNode exitPlan, PlanNode recursivePlan, Builder builder) {
    if (exitPlan.getArity() != recursivePlan.getArity()) {
      throw new IllegalArgumentException(
          "Exit and recursive plans should have the same arity." + "Here: " + exitPlan.getArity() + " vs " + recursivePlan.getArity());
    }

    this.deltaNode = builder.delta;
    this.fullNode = builder.full;

    this.exitPlan = exitPlan;
    // TODO: use a building like in MaterializationNode, in order to avoid performance bugs
    //this.recursivePlan = recursivePlan.replace(delta, deltaNode).replace(full, fullNode);
    this.recursivePlan = recursivePlan;
  }

  /** Use exit plan and recursive plan. The delta is the one in the recursive plan. It will be replaced by a new delta. */
  public RecursionNode(PlanNode exitPlan, PlanNode recursivePlan, PlaceholderNode delta, PlaceholderNode full) {
    if (exitPlan.getArity() != recursivePlan.getArity()) {
      throw new IllegalArgumentException(
          "Exit and recursive plans should have the same arity. " + "Here: " + exitPlan.getArity() + " vs " + recursivePlan.getArity());
    }

    this.deltaNode = new PlaceholderNode("delta", exitPlan.getArity());
    this.fullNode = new PlaceholderNode("full", exitPlan.getArity());
    this.exitPlan = exitPlan;
    // TODO: use a building like in MaterializationNode, in order to avoid performance bugs
    this.recursivePlan = recursivePlan.replace(delta, deltaNode).replace(full, fullNode);
  }

  public PlanNode getExitPlan() {
    return exitPlan;
  }

  public PlanNode getRecursivePlan() {
    return recursivePlan;
  }

  public PlaceholderNode getDelta() {
    return deltaNode;
  }

  public PlaceholderNode getFull() {
    return fullNode;
  }

  @Override
  public int getArity() {
    return exitPlan.getArity();
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
    RecursionNode other = (RecursionNode) obj;
    assumedEqualities = Tools.with(Tools.with(assumedEqualities, deltaNode, other.deltaNode), fullNode, other.fullNode);
    return exitPlan.equals(other.exitPlan, assumedEqualities) && recursivePlan.equals(other.recursivePlan, assumedEqualities);
  }

  @Override
  public int hashCode() {
    return exitPlan.hashCode() ^ 137 * recursivePlan.hashCode();
  }

  @Override
  public String toString() {
    return toPrettyString();
  }

  @Override
  public String operatorString() {
    return "rec_" + hash();
  }

  @Override
  public List<PlanNode> children() {
    return Arrays.asList(exitPlan, recursivePlan);
  }

  @Override
  public List<PlaceholderNode> placeholders() {
    return Arrays.asList(fullNode, deltaNode);
  }

  @Override
  public PlanNode transform(TransformFn fn, List<PlanNode> originalPath) {
    try {
      Tools.addLast(originalPath, this);
      PlanNode.assertSameArity(exitPlan, recursivePlan);

      PlanNode newExit = exitPlan.transform(fn, originalPath);
      PlanNode.assertSameArity(newExit, exitPlan);

      PlanNode newRecursion = recursivePlan.transform(fn, originalPath);
      PlanNode.assertSameArity(newRecursion, recursivePlan);

      PlanNode newNode = newExit.equals(exitPlan) && newRecursion.equals(recursivePlan) ? this : transform(newExit, newRecursion);
      return fn.apply(this, newNode, originalPath);
    } finally {
      Tools.removeLast(originalPath);
    }
  }

  public RecursionNode transform(PlanNode exit, PlanNode recursion) {
    return new RecursionNode(exit, recursion, deltaNode, fullNode);
  }

}
