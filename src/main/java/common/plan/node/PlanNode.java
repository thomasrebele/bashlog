package common.plan.node;

import common.Tools;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A plan node (like a union or a join) following relational algebra.
 * Create a new implementation of this interface to create a new kind of node.
 * Some utility methods are provided to build more easily plans.
 * <p>
 * LogicalPlanBuilder builds a plan from a Program
 */
public interface PlanNode {

  /**
   * Number of columns of the resulting table
   */
  int getArity();

  /**
   * Short string representation for this plan node (e.g. ⋈ for join, ∪ for union)
   */
  String operatorString();

  /**
   * Children of this plan node (without replacement tokens like DeltaNode, ReuseNode)
   */
  Collection<PlanNode> children();

  /** List of placeholder nodes that are associated with this node */
  default List<PlaceholderNode> placeholders() {
    return Collections.emptyList();
  }

  /**
   * Children which should be printed by toPrettyString()
   */
  default Collection<PlanNode> childrenForPrettyString() {
    return children();
  }

  /**
   * Whether the plan always returns zero rows as result. This should only be the case with UnionNode
   */
  default boolean isEmpty() {
    return false;
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode equalityFilter(int field, Comparable<?> value) {
    if (this.isEmpty()) {
      return this;
    }
    return new ConstantEqualityFilterNode(this, field, value);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode equalityFilter(int field1, int field2) {
    if (this.isEmpty()) {
      return this;
    }
    return new VariableEqualityFilterNode(this, field1, field2);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode join(PlanNode other, int[] leftProjection, int[] rightProjection) {
    if (this.isEmpty() || other.isEmpty()) {
      return PlanNode.empty(this.getArity() + other.getArity());
    }
    return new JoinNode(this, other, leftProjection, rightProjection);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode antiJoin(PlanNode other, int[] leftProjection) {
    if (this.isEmpty() || other.isEmpty()) {
      return this;
    }
    return new AntiJoinNode(this, other, leftProjection);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode project(int[] projection) {
    return project(projection, new Comparable[] {});
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode project(int[] projection, Comparable<?>[] constants) {
    if (this.isEmpty()) {
      return empty(Math.max(projection.length, constants.length));
    } else if (Tools.isIdentity(projection, getArity())) {
      return this;
    }

    if (this instanceof ProjectNode) {
      ProjectNode child = (ProjectNode) this;
      projection = Arrays.copyOf(projection, projection.length);
      constants = Arrays.copyOf(constants, constants.length);
      /* Combine proj_{f1=f2}(proj_{f3=f4} ...) to proj_{f1=f2,f3=f4} ... */
      for (int i = 0; i < projection.length; i++) {
        int src = projection[i];
        if (src < 0) {
          continue;
        }
        projection[i] = child.getProjection()[src];
        if (projection[i] < 0) {
          constants[i] = child.getConstant(src).orElseThrow(IllegalStateException::new);
        }
      }
      return new ProjectNode(child.getTable(), projection, constants);
    }

    return new ProjectNode(this, projection, constants);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  static PlanNode empty(int arity) {
    return new UnionNode(arity);
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode union(PlanNode other) {
    return union(Collections.singletonList(other));
  }

  /**
   * Convenience methods to wrap a plan node in another one
   */
  default PlanNode union(Collection<PlanNode> others) {
    Set<PlanNode> elements = new HashSet<>();
    if (this instanceof UnionNode) {
      elements.addAll(this.children());
    } else {
      elements.add(this);
    }
    for (PlanNode other : others) {
      if (other instanceof UnionNode) {
        elements.addAll(other.children());
      } else {
        elements.add(other);
      }
    }
    switch (elements.size()) {
      case 0:
        return PlanNode.empty(this.getArity());
      case 1:
        return elements.iterator().next();
      default:
        return new UnionNode(elements);
    }
  }

  /**
   * Lambda class for applying transformation on plan node
   */
  interface TransformFn {

    /**
     * If transformations need access to the original node or its parent, they need to implement this method
     *
     * @param originalNode   the node that is currently being transformed
     * @param transformed    a new node similar to originalNode (but new instance), where originalNode's children were already transformed
     * @param originalPath   list of nodes that were visited to reach originalNode (inclusive), starting from the node on which transform(...) was called (inclusive)
     *                       The transformation may modify this list. That means, it must be copied if it will be accessed after the transformation
     * @return replacement for current node
     */
    PlanNode apply(PlanNode originalNode, PlanNode transformed, List<PlanNode> originalPath);
  }

  /**
   * Transform operator tree.
   * First transforms children, then applies fn to this node which children have been replaced
   * If you don't want to apply any specific operation just return the parameter <br>
   * Parameters of fn: see {@link TransformFn#apply(PlanNode, PlanNode, List)}
   */
  default PlanNode transform(TransformFn fn, List<PlanNode> originalPath) {
    try {
      Tools.addLast(originalPath, this);
      return fn.apply(this, this, originalPath);
    } finally {
      Tools.removeLast(originalPath);
    }
  }

  /** Get parent from path (e.g., originalPath argument of transform function) */
  static PlanNode parent(List<PlanNode> path) {
    return Tools.index(path, -2);
  }

  /** Generate a nice string for path */
  static String pathToString(List<PlanNode> path) {
    return path.stream().map(p -> p.hash() + " " + p.operatorString()).collect(Collectors.joining("  /  "));
  }

  /**
   * Convenience method. Calls {@link #transform(TransformFn, List)}
   */
  default PlanNode transform(TransformFn fn) {
    return transform(fn, null);
  }

  /**
   * Convenience method. Calls {@link #transform(TransformFn, List)}
   */
  default PlanNode transform(Function<PlanNode, PlanNode> fn) {
    return transform((o, n, p) -> fn.apply(n), null);
  }

  /**
   * Check whether plan node contains 'target' as subplan
   */
  default boolean contains(PlanNode target) {
    boolean[] found = new boolean[] { false };
    transform((node) -> {
      if (node.equals(target)) {
        found[0] = true;
      }
      return node;
    });
    return found[0];
  }

  /**
   * Create a new plan where target subplan is replaced by replacement subplan
   */
  default PlanNode replace(PlanNode target, PlanNode replacement) {
    if (target == replacement) return this;
    return transform((node) -> (node.equals(target)) ? replacement : node);
  }

  /**
   * Generate a tree representation of the plan
   */
  default String toPrettyString() {
    return toPrettyString((node, str) -> str, PlaceholderNode.placeholderToParentMap(this));
  }

  /**
   * Generate a tree representation of the plan
   *
   * @param fn function from (plan node, head line) to actually printed line
   */
  default String toPrettyString(BiFunction<PlanNode, String, String> fn, Map<PlaceholderNode, PlanNode> placeholderToParent) {
    StringBuilder sb = new StringBuilder();
    toPrettyString(sb, "", "", fn, placeholderToParent);
    return sb.toString();
  }

  /**
   * Internal method for generating a tree representation of the plan
   *
   * @param stringBuilder already generated string representation
   * @param prefixHead    prefix for the first line of output (will be adapted for recursive calls)
   * @param prefixOther   prefix for the other lines (will be adapted for recursive calls)
   * @param fn            see {@link #toPrettyString(BiFunction, Map)}
   */
  default void toPrettyString(StringBuilder stringBuilder, String prefixHead, String prefixOther, //
      BiFunction<PlanNode, String, String> fn, Map<PlaceholderNode, PlanNode> placeholderToParent) {
    String operator = operatorString();
    if (this instanceof PlaceholderNode && placeholderToParent != null) {
      PlanNode parent = placeholderToParent.get(this);
      operator += parent == null ? " (PARENT UNKNOWN)" : " for " + parent.operatorString();
    }
    stringBuilder.append(hash()).append(fn.apply(this, " " + prefixHead + operator + " arity " + getArity())).append("\n");
    Collection<PlanNode> args = childrenForPrettyString();
    Iterator<PlanNode> it = args.iterator();
    while(it.hasNext()) {
      PlanNode arg = it.next();
      boolean last = !it.hasNext();
      arg.toPrettyString(stringBuilder, prefixOther + "+-", prefixOther + (last ? "  " : "| "), fn, placeholderToParent);
    }
  }

  /**
   * Almost unique representation of this node, helps for debugging
   */
  default String hash() {
    String hash = Integer.toString(Math.abs(hashCode()));
    if (hash.length() > 4) {
      hash = hash.substring(0, 4);
    } else {
      hash = String.format("%4s", hash);
    }
    return hash;
  }

  /**
   * Determine whether two plans are equal
   * @return true if equal
   */
  default boolean equals(Object other, Map<PlanNode, PlanNode> assumedEqualities) {
    return equals(other);
  }

  /**
   * Height of this plan tree
   */
  default int height() {
    return children().isEmpty() ? 0 : children().stream().mapToInt(PlanNode::height).max().getAsInt() + 1;
  }


}
