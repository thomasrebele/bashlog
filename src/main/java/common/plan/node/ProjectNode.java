package common.plan.node;

import common.Tools;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Restrict a plan node to certain columns, and/or introduce new columns (based on existing columns or constants). */
public class ProjectNode implements PlanNode {

  private final PlanNode table;

  private final int[] projection;

  private final Comparable<?>[] constants;

  /**
   * @param projection position in the new tuple => position in the old tuple. If <0 the value is null (i.e. empty fields)
   * @param constants  position in the new tuple => constant to set
   */
  ProjectNode(PlanNode table, int[] projection, Comparable<?>[] constants) {
    if (Arrays.stream(projection).anyMatch(i -> i >= table.getArity())) {
      throw new IllegalArgumentException("Invalid projection: trying to project a non-existing field: " + Arrays.toString(projection) + "\n" + table
          + ", arity " + table.getArity());
    }
    if (IntStream.range(0, projection.length).anyMatch(i -> projection[i] < 0 && (constants.length <= i || constants[i] == null))) {
      throw new IllegalArgumentException("Invalid projection: trying to project a non-existing field");
    }

    this.table = table;
    this.projection = projection;
    this.constants = constants;
  }

  /** Table the projection applies to */
  public PlanNode getTable() {
    return table;
  }

  /** Get projection columns */
  public int[] getProjection() {
    return projection;
  }

  /** Get projection constants, (used for columns with a constant value) */
  public Comparable<?>[] getConstants() {
    return constants;
  }

  /** Get constant for column i */
  public Optional<Comparable<?>> getConstant(int i) {
    return Tools.get(constants, i);
  }

  @Override
  public int getArity() {
    return Math.max(projection.length, constants.length);
  }

  @Override
  public String toString() {
    return operatorString() + "(" + table.toString() + ")";
  }

  @Override
  public String operatorString() {
    String params = IntStream.range(0, projection.length).mapToObj(i -> {
      if (projection[i] >= 0) {
        if (i == projection[i]) {
          return "" + i;
        } else {
          return Integer.toString(i) + " = " + projection[i];
        }
      } else {
        return Integer.toString(i) + " = " + getConstant(i).map(Object::toString).orElse("null");
      }
    }).filter(p -> !p.isEmpty()).collect(Collectors.joining(", "));
    return "π_{" + params + "}";
  }

  @Override
  public List<PlanNode> children() {
    return Collections.singletonList(table);
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
    ProjectNode node = (ProjectNode) obj;
    return Arrays.equals(projection, node.projection) && Arrays.equals(constants, node.constants) && table.equals(node.table, assumedEqualities);
  }

  @Override
  public int hashCode() {
    return "projection".hashCode() ^ table.hashCode() ^ Arrays.hashCode(projection) ^ Arrays.hashCode(constants);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    PlanNode newTable = table.transform(fn, this);
    PlanNode newNode = newTable.equals(table) ? this : newTable.project(projection, constants);
    return fn.apply(this, newNode, originalParent);
  }

  /** Whether this projection uses any constants */
  public boolean hasConstants() {
    return constants.length != 0 && Arrays.stream(constants).anyMatch(c -> c != null);
  }
}
