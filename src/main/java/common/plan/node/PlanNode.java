package common.plan.node;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A plan node (like a union or a join) following relational algebra.
 * Create a new implementation of this interface to create a new kind of node.
 * Some utility methods are provided to build more easily plans.
 * <p>
 * LogicalPlanBuilder builds a plan from a Program
 */
public interface PlanNode {

  /** Number of columns of the resulting table */
  int getArity();

  /** Short string representation for this plan node (e.g. ⋈ for join, ∪ for union) */
  String operatorString();

  /** Children of this plan node (without replacement tokens like DeltaNode, ReuseNode) */
  List<PlanNode> children();

  /** Whether the plan always returns zero rows as result. This should only be the case with UnionNode */
  default boolean isEmpty() {
    return false;
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode equalityFilter(int field, Comparable<?> value) {
    return new ConstantEqualityFilterNode(this, field, value);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode equalityFilter(int field1, int field2) {
    return new VariableEqualityFilterNode(this, field1, field2);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode join(PlanNode other, int[] leftProjection, int[] rightProjection) {
    return new JoinNode(this, other, leftProjection, rightProjection);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode antiJoin(PlanNode other, int[] leftProjection) {
    return new AntiJoinNode(this, other, leftProjection);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode project(int[] projection) {
    return new ProjectNode(this, projection);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode project(int[] projection, Comparable<?>[] constants) {
    return new ProjectNode(this, projection, constants);
  }

  /** Convenience methods to wrap a plan node in another one */
  static PlanNode empty(int arity) {
    return new UnionNode(arity);
  }

  /** Convenience methods to wrap a plan node in another one */
  default PlanNode union(PlanNode other) {
    return new UnionNode(this, other);
  }

  /** Convenience methods to wrap a plan node in another one */
  default RecursionNode recursion() {
    return new RecursionNode(this);
  }

  /** Lambda class for applying transformation on plan node */
  public interface TransformFn {

    /**
     * If transformations need access to the original node or its parent, they need to implement this method
     * @param originalNode the node that is currently being transformed
     * @param transformed a new node similar to originalNode (but new instance), where originalNode's children were already transformed
     * @param originalParent 
     * @return replacement for current node
     */
    public PlanNode apply(PlanNode originalNode, PlanNode transformed, PlanNode originalParent);
  }

  /**
   * Transform operator tree.
   * First transforms children, then applies fn to this node which children have been replaced
   * If you don't want to apply any specific operation just return the parameter
   * Parameters of fn: (original node, transformed node)
   */
  public default PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, this, originalParent);
  }

  /** Convenience method. Calls {@link #transform(TransformFn, PlanNode)} */
  public default PlanNode transform(TransformFn fn) {
    return transform(fn, null);
  }

  /** Convenience method. Calls {@link #transform(TransformFn, PlanNode)} */
  public default PlanNode transform(Function<PlanNode, PlanNode> fn) {
    return transform((o, n, p) -> fn.apply(n), null);
  }

  /** Check whether plan node contains 'target' as subplan */
  public default boolean contains(PlanNode target) {
    boolean[] found = new boolean[] { false };
    transform((node) -> {
      if (node.equals(target)) {
        found[0] = true;
      }
      return node;
    });
    return found[0];
  }

  /** Create a new plan where target subplan is replaced by replacement subplan */
  public default PlanNode replace(PlanNode target, PlanNode replacement) {
    return transform((node) -> (node.equals(target)) ? replacement : node);
  }

  /** Generate a tree representation of the plan */
  default String toPrettyString() {
    return toPrettyString((node, str) -> str);
  }

  /** Generate a tree representation of the plan
   * @param fn function from (plan node, head line) to actually printed line */
  default String toPrettyString(BiFunction<PlanNode, String, String> fn) {
    StringBuilder sb = new StringBuilder();
    toPrettyString(sb, "", "", fn);
    return sb.toString();
  }

  /**
   * Internal method for generating a tree representation of the plan
   * @param stringBuilder already generated string representation
   * @param prefixHead prefix for the first line of output (will be adapted for recursive calls) 
   * @param prefixOther prefix for the other lines (will be adapted for recursive calls)
   * @param fn see {@link #toPrettyString(BiFunction)}
   */
  default void toPrettyString(StringBuilder stringBuilder, String prefixHead, String prefixOther, BiFunction<PlanNode, String, String> fn) {
    stringBuilder.append(hash()).append(fn.apply(this, " " + prefixHead + operatorString() + " arity " + getArity())).append("\n");
    List<PlanNode> args = children();
    for (int i = 0; i < args.size(); i++) {
      PlanNode arg = args.get(i);
      boolean last = i == args.size() - 1;
      arg.toPrettyString(stringBuilder, prefixOther + "+-", prefixOther + (last ? "  " : "| "), fn);
    }
  }

  /** Almost unique representation of this node, helps for debugging */
  default String hash() {
    String hash = Integer.toString(Math.abs(hashCode()));
    if (hash.length() > 4) {
      hash = hash.substring(0, 4);
    } else {
      hash = String.format("%4s", hash);
    }
    return hash;
  }
}
