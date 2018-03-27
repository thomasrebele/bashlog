package sparqlog;

public class SparqlogCompiler {

  /*private static final Logger LOGGER = LoggerFactory.getLogger(SparqlogCompiler.class);
  private static final Set<String> BUILDS_IN = Collections.singleton("fact");
  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new SimplifyRecursion(), new ReorderJoinLinear(), new PushDownFilterAndProject());

  private static final String TABLE_ALIAS_PREFIX = "T";

  private Map<PlaceholderNode, PlanNode> placeholderToParent = new HashMap<>();
  private Map<PlanNode, String> closureTables = new HashMap<>();
  private int count = 0;

  public String compile(Program program, String tripleRelation, String relationToOutput) {
    LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(BUILDS_IN, Collections.singleton(relationToOutput));
    PlanNode plan = optimize(planBuilder.getPlanForProgram(program).get(relationToOutput));
    placeholderToParent = PlaceholderNode.placeholderToParentMap(plan);
    return mapPlanNode(plan).toString();
  }

  private PlanNode optimize(PlanNode node) {
    for (Optimizer optimizer : OPTIMIZERS) {
      node = optimizer.apply(node);
    }
    return node;
  }

  private Select mapPlanNode(PlanNode node) {
    if (node instanceof ConstantEqualityFilterNode) {
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

  private Select mapConstantEqualityFilterNode(ConstantEqualityFilterNode node) {
    Select parent = mapPlanNode(node.getTable());
    return parent.withWhere("FILTER(" + parent.select.get(node.getField()) + " = \"" + node.getValue() + "\")");
  }

  private Select mapJoinNode(JoinNode node) {
    Select left = mapPlanNode(node.getLeft());
    Select right = mapPlanNode(node.getRight());
    Select result = new Select(merge(left.select, right.select), merge(left.where, right.where));
    for (int i = 0; i < node.getLeftProjection().length; i++) {
      result.where.add("FILTER(" + left.select.get(node.getLeftProjection()[i]) + " = " + right.select.get(node.getRightProjection()[i]) + ")");
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
        select.add("BIND(\"" + constants[i].toString() + "\" AS " + newVariable() + ")"); //TODO won't work everywhere
      } else {
        throw new IllegalArgumentException("No value for tuple argument");
      }
    }
    return new Select(select, parent.where);
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
            .map(child -> "{" + mapPlanNode(child).toString() + "}")
        .collect(Collectors.joining("\n UNION "));
    return newTable(union, node.getArity());
  }

  private Select mapVariableEqualityFilterNode(VariableEqualityFilterNode node) {
    Select parent = mapPlanNode(node.getTable());
    return parent.withWhere(parent.select.get(node.getField1()) + " = '" + parent.select.get(node.getField2()) + "'");
  }

  private String toCol(String table, int pos) {
    return table + ".C" + pos;
  }

  private String newVariable() {
    return "v" + (count++);
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
    Set<String> where = Collections.emptySet();

    Select(List<String> select, Set<String> where) {
      this.select = select;
      this.where = where;
    }

    Select(List<String> select, String from) {
      this.select = select;
    }

    Select withWhere(String cond) {
      return new Select(select, addToSet(where, cond));
    }

    private <T> Set<T> addToSet(Set<T> ens, T ele) {
      Set<T> n = new HashSet<>(ens);
      n.add(ele);
      return n;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      String selectColumns = IntStream.range(0, select.size()).mapToObj(i -> select.get(i) + " AS C" + i).collect(Collectors.joining(", "));
      builder.append("\n SELECT DISTINCT ").append(selectColumns)
              .append(" FROM ").append(String.join(", ", from));
      if (!where.isEmpty()) {
        builder.append(" WHERE {\n").append(String.join(".\n", where)).append("\n}");
      }
      return builder.toString();
    }
  }*/
}
