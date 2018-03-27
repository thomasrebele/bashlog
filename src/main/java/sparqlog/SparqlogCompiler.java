package sparqlog;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


import common.parser.Program;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.Optimizer;
import common.plan.optimizer.PushDownFilterAndProject;
import common.plan.optimizer.ReorderJoinLinear;
import common.plan.optimizer.SimplifyRecursion;

public class SparqlogCompiler {

  private static final Set<String> BUILDS_IN = Collections.singleton("fact");

  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new SimplifyRecursion(), new ReorderJoinLinear(), new PushDownFilterAndProject());

  int count = 0;

  private PlanNode optimize(PlanNode node) {
    for (Optimizer optimizer : OPTIMIZERS) {
      node = optimizer.apply(node);
    }
    return node;
  }

  public String compile(Program program, String string, String relationToOutput) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(BUILDS_IN, Collections.singleton(relationToOutput));
    PlanNode plan = optimize(planBuilder.getPlanForProgram(program).get(relationToOutput));

    String[] vars = IntStream.range(0, plan.getArity()).mapToObj(i -> "?v" + i).toArray((i) -> new String[i]);
    List<String> s = mapPlanNode(plan, vars);

    StringBuilder sparql = new StringBuilder();
    sparql.append("SELECT DISTINCT ");
    sparql.append(Arrays.stream(vars).collect(Collectors.joining(" ")));

    sparql.append(" WHERE { \n");
    s.stream().map(k -> "  " + k).forEach(sparql::append);
    sparql.append("}");

    return sparql.toString();
  }

  /**
   * Convert a plan node to sparql snippets
   * @param plan
   * @param colToVar which variable should be used for which column
   * @return
   */
  private List<String> mapPlanNode(PlanNode plan, String[] colToVar) {
    if (plan instanceof ProjectNode) {
      ProjectNode n = (ProjectNode) plan;
      String[] newColToVar = new String[n.getTable().getArity()];
      for(int i=0; i<n.getProjection().length; i++) {
        newColToVar[n.getProjection()[i]] = colToVar[i];
      }
      return mapPlanNode(((ProjectNode) plan).getTable(), newColToVar);

    } else if (plan instanceof ConstantEqualityFilterNode) {
      ConstantEqualityFilterNode n = (ConstantEqualityFilterNode) plan;
      colToVar[n.getField()] = n.getValue().toString();
      return mapPlanNode(n.getTable(), colToVar);

    } else if (plan instanceof JoinNode) {
      JoinNode n = (JoinNode) plan;
      int leftArity = n.getLeft().getArity();
      String[] leftColToVar = new String[n.getLeft().getArity()];
      String[] rightColToVar = new String[n.getLeft().getArity()];

      System.arraycopy(colToVar, 0, leftColToVar, 0, n.getLeft().getArity());
      System.arraycopy(colToVar, leftArity, rightColToVar, 0, n.getRight().getArity());

      for (int i = 0; i < n.getLeftProjection().length; i++) {
        int leftField = n.getLeftProjection()[i];
        int rightField = n.getRightProjection()[i] ;
        int outRightField = rightField + leftArity;

        String var;
        if (colToVar[leftField] == null && colToVar[outRightField] == null) {
          var = "?t" + count++;
        } else if (colToVar[leftField] != null || colToVar[outRightField] != null) {
          var = Optional.ofNullable(colToVar[leftField]).orElse(colToVar[outRightField]);
        } else {
          throw new IllegalStateException("unsupported");
        }
        leftColToVar[leftField] = var;
        rightColToVar[rightField] = var;
      }

      return Stream.concat(mapPlanNode(n.getLeft(), leftColToVar).stream(), mapPlanNode(n.getRight(), rightColToVar).stream())
          .collect(Collectors.toList());

    } else if (plan instanceof UnionNode) {
      List<String> result = new ArrayList<>();
      result.add("{ { \n");
      String sep = "";
      for (PlanNode child : plan.children()) {
        result.add(sep);
        sep = "} UNION { \n ";
        result.addAll(mapPlanNode(child, colToVar));
      }
      result.add("} } \n");

      return result;

    } else if (plan instanceof BashNode) {
      if (colToVar.length != 3) {
        throw new UnsupportedOperationException("can only translate triples");
      }
      return Collections.singletonList(Arrays.stream(colToVar).map(var -> var == null ? "[]" : var).collect(Collectors.joining(" ")) + " .\n");
    } else {
      throw new UnsupportedOperationException(plan.getClass() + " not supported");
    }

  }

}
