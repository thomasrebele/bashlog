package common.plan.node;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Restrict a plan node to certain columns, and/or introduce new columns (based on existing columns or constants). */
public class ProjectNode implements PlanNode {

  private static final Comparable<?>[] EMPTY_CONSTANTS = new Comparable[0];

  private final PlanNode table;

  private final int[] projection;

  private final Comparable<?>[] constants;

  /**
   * @param projection position in the new tuple => position in the old tuple. If <0 the value is null (i.e. empty fields)
   * @param constants  position in the new tuple => constant to set
   */
  public ProjectNode(PlanNode table, int[] projection, Comparable<?>[] constants) {
    if (Arrays.stream(projection).anyMatch(i -> i >= table.getArity())) {
      throw new IllegalArgumentException(
          "Invalid projection: try to project a not existing field: " + Arrays.toString(projection) + "\n" + table + ", arity " + table.getArity());
    }
    if (IntStream.range(0, projection.length).anyMatch(i -> projection[i] < 0 && (constants.length <= i || constants[i] == null))) {
      throw new IllegalArgumentException("Invalid projection: try to project a not existing field");
    }

    this.table = table;
    this.projection = projection;
    this.constants = constants;
  }

  /** A projection whose column indices are given by array 'projection' */
  public ProjectNode(PlanNode table, int[] projection) {
    this(table, projection, EMPTY_CONSTANTS);
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
    if (constants.length > i && constants[i] != null) {
      return Optional.of(constants[i]);
    } else {
      return Optional.empty();
    }
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
    if (!(obj.getClass() == getClass())) {
      return false;
    }
    ProjectNode node = (ProjectNode) obj;
    return table.equals(node.table) && Arrays.equals(projection, node.projection) && Arrays.equals(constants, node.constants);
  }

  @Override
  public int hashCode() {
    return "projection".hashCode() ^ table.hashCode() ^ Arrays.hashCode(projection) ^ Arrays.hashCode(constants);
  }

  @Override
  public PlanNode transform(TransformFn fn, PlanNode originalParent) {
    return fn.apply(this, new ProjectNode(table.transform(fn, this), projection, constants), originalParent);
  }

  /** Whether this projection uses any constants */
  public boolean hasConstants() {
    return constants.length != 0 && Arrays.stream(constants).anyMatch(c -> c != null);
  }
}
