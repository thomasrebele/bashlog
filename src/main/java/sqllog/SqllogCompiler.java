package sqllog;

import common.parser.*;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.Optimizer;
import common.plan.optimizer.PushDownFilterAndProject;
import common.plan.optimizer.SimplifyPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqllogCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqllogCompiler.class);
  private static final Set<String> BUILDS_IN = Collections.singleton("sql_table");
  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new SimplifyPlan(), new PushDownFilterAndProject(), new SimplifyPlan());

  private Map<PlanNode, String> closureTables = new HashMap<>();
  private int count = 0;

  public String compile(Program program, Set<String> relationsInTables, String relationToOutput) {
    relationsInTables.forEach(relation -> program.addRule(buildLoadRuleForRelation(relation)));

    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(BUILDS_IN, Collections.singleton(relationToOutput));
    return mapPlanNode(optimize(planBuilder.getPlanForProgram(program).get(relationToOutput))).toString();
  }

  private PlanNode optimize(PlanNode node) {
    for (Optimizer optimizer : OPTIMIZERS) {
      node = optimizer.apply(node);
    }
    return node;
  }

  private Rule buildLoadRuleForRelation(String relation) {
    String rel = relation.split("/")[0];
    int arity = Integer.parseInt(relation.split("/")[1]);
    Variable[] variables = IntStream.range(0, arity).mapToObj(i -> new Variable(Integer.toString(i))).toArray(Variable[]::new);
    return new Rule(
            new CompoundTerm(rel, variables),
            new CompoundTerm("sql_table", new Constant<>(relation), new TermList(variables))
    );
  }

  private Select mapPlanNode(PlanNode node) {
    if (node instanceof BuiltinNode) {
      return mapBuiltinNode((BuiltinNode) node);
    } else if (node instanceof ConstantEqualityFilterNode) {
      return mapConstantEqualityFilterNode((ConstantEqualityFilterNode) node);
    } else if (node instanceof JoinNode) {
      return mapJoinNode((JoinNode) node);
    } else if (node instanceof ProjectNode) {
      return mapProjectNode((ProjectNode) node);
    } else if (node instanceof RecursionNode) {
      return mapRecursionNode((RecursionNode) node);
    } else if (node instanceof PlaceholderNode) {
      return mapTokenNode((PlaceholderNode) node);
    } else if (node instanceof UnionNode) {
      return mapUnionNode((UnionNode) node);
    } else if (node instanceof VariableEqualityFilterNode) {
      return mapVariableEqualityFilterNode((VariableEqualityFilterNode) node);
    } else {
      throw new IllegalArgumentException("Unknown node type: " + node.toString());
    }
  }

  private Select mapBuiltinNode(BuiltinNode node) {
    String name = node.compoundTerm.name;
    switch (name) {
      case "sql_table":
        String[] relation = ((String) ((Constant) node.compoundTerm.args[0]).getValue()).trim().split("/");
        return newTable(relation[0], Integer.parseInt(relation[1]));
      default:
        throw new IllegalArgumentException("Unsupported build-in: " + node.toString());
    }
  }

  private Select mapConstantEqualityFilterNode(ConstantEqualityFilterNode node) {
    Select parent = mapPlanNode(node.getTable());
    return parent.withWhere(parent.select.get(node.getField()) + " = '" + node.getValue() + "'");
  }

  private Select mapJoinNode(JoinNode node) {
    Select left = mapPlanNode(node.getLeft());
    Select right = mapPlanNode(node.getRight());
    Select result = new Select(merge(left.select, right.select), merge(left.from, right.from), merge(left.where, right.where), merge(left.recursions, right.recursions));
    for (int i = 0; i < node.getLeftJoinProjection().length; i++) {
      result.where.add(left.select.get(node.getLeftJoinProjection()[i]) + " = " + right.select.get(node.getRightJoinProjection()[i]));
    }
    return result;
  }

  private Select mapProjectNode(ProjectNode node) {
    Select parent = mapPlanNode(node.getTable());
    int[] projection = node.getProjection();
    Comparable[] constants = node.getConstants();
    List<String> select = new ArrayList<>(projection.length);
    for (int i = 0; i < projection.length; i++) {
      if (projection[i] >= 0) {
        select.add(parent.select.get(projection[i]));
      } else if (constants.length > i) {
        select.add("('" + constants[i].toString() + "' AS " + newAlias() + ")"); //TODO won't work everywhere
      } else {
        throw new IllegalArgumentException("No value for tuple argument");
      }
    }
    return new Select(select, parent.from, parent.where, parent.recursions);
  }

  private Select mapRecursionNode(RecursionNode node) {
    String closureTable = newAlias();
    closureTables.put(node, closureTable);

    Select exit = mapPlanNode(node.getExitPlan());
    Select rec = mapPlanNode(node.getRecursivePlan());

    String fields = IntStream.range(0, node.getArity())
            .mapToObj(i -> "C" + i)
            .collect(Collectors.joining(", "));
    String recursion = "RECURSIVE " + closureTable + "(" + fields + ") AS ((" + exit.toString() + ") UNION ALL (" + rec.toString() + "))";

    Select end = newTable(closureTable, node.getArity());
    end.recursions.add(recursion);
    return end;
  }

  private Select mapTokenNode(PlaceholderNode node) {
    return newTable(closureTables.get(node.getParent()), node.getArity());
  }

  private Select mapUnionNode(UnionNode node) {
    String union = node.getChildren().stream()
            .map(child -> "(" + mapPlanNode(child).toString() + ")")
            .collect(Collectors.joining(" UNION ALL "));
    return newTable("(" + union + ")", node.getArity());
  }

  private Select mapVariableEqualityFilterNode(VariableEqualityFilterNode node) {
    Select parent = mapPlanNode(node.getTable());
    return parent.withWhere(parent.select.get(node.getField1()) + " = '" + parent.select.get(node.getField2()) + "'");
  }

  private String toCol(String table, int pos) {
    return table + ".C" + pos;
  }

  private String newAlias() {
    count++;
    return "T" + count;
  }

  private <T> List<T> merge(List<T> a, List<T> b) {
    List<T> res = new ArrayList<>(a);
    res.addAll(b);
    return res;
  }

  private <T> Set<T> merge(Set<T> a, Set<T> b) {
    Set<T> res = new HashSet<>(a);
    res.addAll(b);
    return res;
  }

  private Select newTable(String table, int arity) {
    String alias = newAlias();
    List<String> select = IntStream.range(0, arity)
            .mapToObj(i -> toCol(alias, i))
            .collect(Collectors.toList());
    return new Select(select, table + " AS " + alias);
  }

  private static class Select {
    List<String> select;
    Set<String> from;
    Set<String> where = Collections.emptySet();
    Set<String> recursions = new HashSet<>();

    Select(List<String> select, Set<String> from, Set<String> where, Set<String> recursions) {
      this.select = select;
      this.from = from;
      this.where = where;
      this.recursions = recursions;
    }

    Select(List<String> select, String from) {
      this.select = select;
      this.from = Collections.singleton(from);
    }

    Select withWhere(String cond) {
      return new Select(select, from, addToSet(where, cond), recursions);
    }

    private <T> Set<T> addToSet(Set<T> ens, T ele) {
      Set<T> n = new HashSet<>(ens);
      n.add(ele);
      return n;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      if (!recursions.isEmpty()) {
        builder.append("WITH ").append(String.join(", ", recursions));
      }
      String selectColumns = IntStream.range(0, select.size()).mapToObj(i -> select.get(i) + " AS C" + i).collect(Collectors.joining(", "));
      builder.append(" SELECT ").append(selectColumns)
              .append(" FROM ").append(String.join(", ", from));
      if (!where.isEmpty()) {
        builder.append(" WHERE ").append(String.join(" and ", where));
      }
      return builder.toString().trim();
    }
  }
}
