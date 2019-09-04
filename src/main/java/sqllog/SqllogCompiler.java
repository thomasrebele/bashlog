package sqllog;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.parser.*;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.*;

public class SqllogCompiler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqllogCompiler.class);
  private static final Set<String> BUILDS_IN = Collections.singleton("sql_table");
  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new SimplifyRecursion(), new ReorderJoinLinear(), new PushDownFilterAndProject());

  private static final String TABLE_ALIAS_PREFIX = "T";

  private Map<PlaceholderNode, PlanNode> placeholderToParent = new HashMap<>();
  private Map<PlanNode, String> closureTables = new HashMap<>();
  private int count = 0;

  /* Compability mode for NoDB */
  /** Use WITH T1 AS (...) instead of SELECT ... FROM ... AS T1 */
  private final boolean useWithForAliases;

  /* Compability mode for NoDB */
  /** Use WITH RECURSIVE ... instead of WITH ..., RECURSIVE T1 ... */
  private final boolean recursiveDirectlyAfterWith;

  public SqllogCompiler() {
    useWithForAliases = false;
    recursiveDirectlyAfterWith = false;
  }

  public SqllogCompiler(boolean useWithForAliases, boolean recursiveDirectlyAfterWith) {
    this.useWithForAliases = useWithForAliases;
    this.recursiveDirectlyAfterWith = recursiveDirectlyAfterWith;
  }

  public String compile(Program program, Set<String> relationsInTables, String relationToOutput) {
    relationsInTables.forEach(relation -> program.addRule(buildLoadRuleForRelation(relation)));

    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(BUILDS_IN, Collections.singleton(relationToOutput));
    PlanNode plan = optimize(planBuilder.getPlanForProgram(program).get(relationToOutput));
    placeholderToParent = common.plan.node.PlaceholderNode.placeholderToParentMap(plan);
    return mapPlanNode(plan).toString();
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
      throw new IllegalArgumentException("Unknown node type: " + node.toString() + " operator " + node.operatorString());
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
    for (int i = 0; i < node.getLeftProjection().length; i++) {
      result.where.add(left.select.get(node.getLeftProjection()[i]) + " = " + right.select.get(node.getRightProjection()[i]));
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
    String recursion = closureTable + "(" + fields + ") AS ((" + exit.toString() + ") UNION (" + rec.toString() + "))";
    if (!recursiveDirectlyAfterWith) {
      recursion = "RECURSIVE " + recursion;
    }

    Select end = newTable(closureTable, node.getArity());
    end.recursions.add(recursion);
    return end;
  }

  private Select mapTokenNode(PlaceholderNode node) {
    return newTable(closureTables.get(placeholderToParent.get(node)), node.getArity());
  }

  private Select mapUnionNode(UnionNode node) {
    String union = node.getChildren().stream()
            .map(child -> "(" + mapPlanNode(child).toString() + ")")
        .collect(Collectors.joining("\n UNION ALL "));
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
    return TABLE_ALIAS_PREFIX + count;
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
    if (table.trim().startsWith(TABLE_ALIAS_PREFIX)) {
      alias = table;
    }
    String fAlias = alias;

    List<String> select = IntStream.range(0, arity)
        .mapToObj(i -> toCol(fAlias, i))
            .collect(Collectors.toList());
    if (useWithForAliases) {
      Select s = new Select(select, alias);
      if (table.trim().startsWith(TABLE_ALIAS_PREFIX)) {
      } else if (table.trim().startsWith("(")) {
        s.recursions.add(alias + " AS " + table + "");
      } else {
        s.recursions.add(alias + " AS (SELECT DISTINCT * FROM " + table + ")");
      }
      return s;
    }
    else {
      return new Select(select, table + " AS " + alias);
    }
  }

  private class Select {
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
      return toString("");
    }

    public String toString(String prefix) {
      StringBuilder builder = new StringBuilder();
      if (!recursions.isEmpty()) {
        builder.append("\n WITH ");
        if (SqllogCompiler.this.recursiveDirectlyAfterWith) {
          builder.append("RECURSIVE ");
        }
        builder.append(String.join(", ", recursions));
      }
      String selectColumns = IntStream.range(0, select.size()).mapToObj(i -> select.get(i) + " AS C" + i).collect(Collectors.joining(", "));
      builder.append("\n SELECT DISTINCT ").append(selectColumns)
              .append(" FROM ").append(String.join(", ", from));
      if (!where.isEmpty()) {
        builder.append(" WHERE ").append(String.join(" and ", where));
      }
      return builder.toString();
    }
  }
}
