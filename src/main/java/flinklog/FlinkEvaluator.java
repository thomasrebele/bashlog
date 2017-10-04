package flinklog;

import common.parser.*;
import common.plan.*;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Evaluates a Program on a FactsSet and returns a new FactsSet
 * See flinklog.IntegrationTests for examples of programs.
 */
public class FlinkEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEvaluator.class);
  private static final int MAX_ITERATION = Integer.MAX_VALUE;
  private static final Set<String> BUILDS_IN = Sets.newHashSet("flink_entry_values", "bash_command");
  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new PlanSimplifier(), new PushDownFilterOptimizer(), new PlanSimplifier());

  private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private FactsSet factsSet;
  private Map<PlanNode, Optional<DataSet<FlinkRow>>> cache = new HashMap<>();

  public FlinkEvaluator() {
    env.getConfig().enableObjectReuse();
  }

  private static <T> Stream<T> stream(Optional<T> optional) {
    //TODO: remove when targeting JDK 9+
    return optional.map(Stream::of).orElse(Stream.empty());
  }

  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) {
    //Initialize globals
    factsSet = facts;

    //We to program the loading from the factset
    facts.getRelations().forEach(relation -> program.addRule(buildLoadRuleForRelation(relation)));

    SimpleFactsSet result = new SimpleFactsSet();
    (new LogicalPlanBuilder(BUILDS_IN, relationsToOutput)).getPlanForProgram(program).entrySet().stream()
            .flatMap(relationPlan -> {
              String relation = relationPlan.getKey();
              PlanNode plan = optimize(relationPlan.getValue());
              LOGGER.info("Evaluating relation " + relation + " with plan:\n" + plan.toPrettyString());
              return stream(this.mapPlanNode(plan).map(dataSet ->
                      (DataSet<Tuple2<String, Comparable[]>>) dataSet.map(new MapFunction<FlinkRow, Tuple2<String, Comparable[]>>() {
                        @Override
                        public Tuple2<String, Comparable[]> map(FlinkRow row) throws Exception {
                          Comparable[] args = new Comparable[row.getArity()];
                          for (int i = 0; i < args.length; i++) {
                            args[i] = row.getField(i);
                          }
                          return new Tuple2<>(relation, args);
                        }
                      })
              ));
            })
            .reduce(DataSet::union)
            .ifPresent(dataSet -> {
              try {
                dataSet.collect().forEach(tuple -> result.add(tuple.f0, tuple.f1));
              } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
              }
            });
    return result;
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
            new CompoundTerm("flink_entry_values", new Constant<>(relation), new TermList(variables))
    );
  }

  private Optional<DataSet<FlinkRow>> mapPlanNode(PlanNode planNode) {
    if (!cache.containsKey(planNode)) {
      cache.put(planNode, buildForPlanNode(planNode));
    }
    return cache.get(planNode);
  }

  private Optional<DataSet<FlinkRow>> buildForPlanNode(PlanNode node) {
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
    } else if (node instanceof UnionNode) {
      return mapUnionNode((UnionNode) node);
    } else if (node instanceof VariableEqualityFilterNode) {
      return mapVariableEqualityFilterNode((VariableEqualityFilterNode) node);
    } else {
      throw new IllegalArgumentException("Unknown node type: " + node.toString());
    }
  }

  private Optional<DataSet<FlinkRow>> mapBuiltinNode(BuiltinNode node) {
    String name = node.compoundTerm.name;
    switch (name) {
      case "flink_entry_values":
        String relation = ((String) ((Constant) node.compoundTerm.args[0]).getValue()).trim();
        //TODO: do not materialize here
        List<FlinkRow> tuples = factsSet.getByRelation(relation).map(FlinkRow::new).collect(Collectors.toList());
        if (tuples.isEmpty()) {
          LOGGER.info("Empty relation: " + relation);
          return Optional.empty();
        } else {
          return Optional.of(env.fromCollection(tuples));
        }
      case "bash_command":
        String command = ((String) ((Constant) node.compoundTerm.args[0]).getValue()).trim();
        if (command.startsWith("cat")) {
          Path file = Paths.get(command.substring(4).trim()).toAbsolutePath();
          Pattern pattern = Pattern.compile("[ \t\r\n]");
          return Optional.of(env.readTextFile("file://" + file.toString()).map(line ->
                  new FlinkRow(Arrays.stream(pattern.split(line)))
          ));
        } else {
          throw new IllegalArgumentException("Unsupported bash command: " + node.toString());
        }
      default:
        throw new IllegalArgumentException("Unsupported build-in: " + node.toString());
    }
  }

  private Optional<DataSet<FlinkRow>> mapConstantEqualityFilterNode(ConstantEqualityFilterNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int field = node.getField();
      Comparable value = node.getValue();
      return dataSet.filter(row -> {
        return value.equals(row.getField(field));
      });
    });
  }

  private KeySelector<FlinkRow, FlinkRow> selectorForColumns(int columns[]) {
    return (KeySelector<FlinkRow, FlinkRow>) full -> {
      FlinkRow filtered = new FlinkRow(columns.length);
      for (int i = 0; i < columns.length; i++) {
        filtered.setField(i, full.getField(columns[i]));
      }
      return filtered;
    };
  }

  private Optional<DataSet<FlinkRow>> mapJoinNode(JoinNode node) {
    return mapPlanNode(node.getLeft()).flatMap(left ->
            mapPlanNode(node.getRight()).map(right ->
                    left.join(right)
                            .where(selectorForColumns(node.getLeftJoinProjection()))
                            .equalTo(selectorForColumns(node.getRightJoinProjection()))
                            .with(FlinkRow::concat)));
  }

  private Optional<DataSet<FlinkRow>> mapProjectNode(ProjectNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int[] projection = node.getProjection();
      Comparable[] constants = node.getConstants();
      return dataSet.map(row -> {
        int arity = projection.length;
        FlinkRow result = new FlinkRow(arity);
        for (int i = 0; i < arity; i++) {
          if (projection[i] >= 0) {
            result.setField(i, row.getField(projection[i]));
          } else if (constants.length > i) {
            result.setField(i, constants[i]);
          }
        }
        return result;
      });
    });
  }

  private Optional<DataSet<FlinkRow>> mapRecursionNode(RecursionNode node) {
    return mapPlanNode(node.getExitPlan()).map(initialSet -> {
      DataSet<Tuple1<FlinkRow>> initialSetTuple = toTuple(initialSet);
      DeltaIteration<Tuple1<FlinkRow>, FlinkRow> iteration = initialSetTuple.iterateDelta(initialSet, MAX_ITERATION, 0);
      cache.put(node.getDelta(), Optional.of(iteration.getWorkset()));
      cache.put(node.getFull(), Optional.of(fromTuple(iteration.getSolutionSet())));
      return mapPlanNode(node.getRecursivePlan())
              .map(recursivePlan -> fromTuple(iteration.closeWith(toTuple(recursivePlan), recursivePlan)))
              .orElse(initialSet);
    });
  }

  private DataSet<Tuple1<FlinkRow>> toTuple(DataSet<FlinkRow> dataSet) {
    return dataSet.map(new MapFunction<FlinkRow, Tuple1<FlinkRow>>() {
      @Override
      public Tuple1<FlinkRow> map(FlinkRow value) throws Exception {
        return new Tuple1<>(value);
      }
    });
  }

  private DataSet<FlinkRow> fromTuple(DataSet<Tuple1<FlinkRow>> dataSet) {
    return dataSet.map(new MapFunction<Tuple1<FlinkRow>, FlinkRow>() {
      @Override
      public FlinkRow map(Tuple1<FlinkRow> value) throws Exception {
        return value.f0;
      }
    });
  }

  private Optional<DataSet<FlinkRow>> mapUnionNode(UnionNode node) {
    return node.getChildren().stream()
            .flatMap(child -> stream(this.mapPlanNode(child)))
            .reduce(DataSet::union);
  }

  private Optional<DataSet<FlinkRow>> mapVariableEqualityFilterNode(VariableEqualityFilterNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int field1 = node.getField1();
      int field2 = node.getField2();
      return dataSet.filter(row -> {
        Object v1 = row.getField(field1);
        Object v2 = row.getField(field2);
        return v1 == null ? v2 == null : v1.equals(v2);
      });
    });
  }
}
