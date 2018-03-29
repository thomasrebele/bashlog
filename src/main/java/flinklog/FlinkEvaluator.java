package flinklog;

import common.Evaluator;
import common.FactsSet;
import common.SimpleFactsSet;
import common.parser.*;
import common.plan.LogicalPlanBuilder;
import common.plan.node.*;
import common.plan.optimizer.Optimizer;
import common.plan.optimizer.PushDownFilterAndProject;
import common.plan.optimizer.ReorderJoinLinear;
import common.plan.optimizer.SimplifyRecursion;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.*;
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
public class FlinkEvaluator implements Evaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEvaluator.class);
  private static final int MAX_ITERATION = Integer.MAX_VALUE;
  private static final Set<String> BUILDS_IN = Sets.newHashSet("flink_entry_values", "bash_command");
  private static final List<Optimizer> OPTIMIZERS = Arrays.asList(new SimplifyRecursion(), new ReorderJoinLinear(), new PushDownFilterAndProject(), new SimplifyRecursion());

  private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private FactsSet factsSet;
  private Map<PlanNode, Optional<DataSet<Tuple>>> cache;

  public FlinkEvaluator() {
    env.getConfig().enableObjectReuse();
  }

  @SafeVarargs
  private static <T> Tuple newTuple(T... values) {
    switch (values.length) {
      case 0:
        return Tuple0.INSTANCE;
      case 1:
        return Tuple1.of(values[0]);
      case 2:
        return Tuple2.of(values[0], values[1]);
      case 3:
        return Tuple3.of(values[0], values[1], values[2]);
      case 4:
        return Tuple4.of(values[0], values[1], values[2], values[3]);
      case 5:
        return Tuple5.of(values[0], values[1], values[2], values[3], values[4]);
      case 6:
        return Tuple6.of(values[0], values[1], values[2], values[3], values[4], values[5]);
      case 7:
        return Tuple7.of(values[0], values[1], values[2], values[3], values[4], values[5], values[6]);
      case 8:
        return Tuple8.of(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]);
      case 9:
        return Tuple9.of(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]);
      case 10:
        return Tuple10.of(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]);
      default:
        throw new IllegalArgumentException("This arity is not supported");
    }
  }

  private static Tuple concatTuples(Tuple t1, Tuple t2) {
    Tuple result = newTuple(t1.getArity() + t2.getArity());
    for (int i = 0; i < t1.getArity(); i++) {
      result.setField(t1.getField(i), i);
    }
    for (int i = 0; i < t2.getArity(); i++) {
      result.setField(t2.getField(i), t1.getArity() + i);
    }
    return result;
  }

  private static <T> Stream<T> stream(Optional<T> optional) {
    //TODO: remove when targeting JDK 9+
    return optional.map(Stream::of).orElse(Stream.empty());
  }

  private PlanNode optimize(PlanNode node) {
    for (Optimizer optimizer : OPTIMIZERS) {
      node = optimizer.apply(node);
    }
    return node;
  }

  private static <T> Tuple newTuple(int arity) {
    switch (arity) {
      case 0:
        return new Tuple0();
      case 1:
        return new Tuple1<T>();
      case 2:
        return new Tuple2<T, T>();
      case 3:
        return new Tuple3<T, T, T>();
      case 4:
        return new Tuple4<T, T, T, T>();
      case 5:
        return new Tuple5<T, T, T, T, T>();
      case 6:
        return new Tuple6<T, T, T, T, T, T>();
      case 7:
        return new Tuple7<T, T, T, T, T, T, T>();
      case 8:
        return new Tuple8<T, T, T, T, T, T, T, T>();
      case 9:
        return new Tuple9<T, T, T, T, T, T, T, T, T>();
      case 10:
        return new Tuple10<T, T, T, T, T, T, T, T, T, T>();
      default:
        throw new IllegalArgumentException("This arity is not supported");
    }
  }

  private static TypeInformation typeInfo(int arity) {
    switch (arity) {
      case 0:
        return TypeInformation.of(new TypeHint<Tuple0>() {
        });
      case 1:
        return TypeInformation.of(new TypeHint<Tuple1<Comparable>>() {
        });
      case 2:
        return TypeInformation.of(new TypeHint<Tuple2<Comparable, Comparable>>() {
        });
      case 3:
        return TypeInformation.of(new TypeHint<Tuple3<Comparable, Comparable, Comparable>>() {
        });
      case 4:
        return TypeInformation.of(new TypeHint<Tuple4<Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 5:
        return TypeInformation.of(new TypeHint<Tuple5<Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 6:
        return TypeInformation.of(new TypeHint<Tuple6<Comparable, Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 7:
        return TypeInformation.of(new TypeHint<Tuple7<Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 8:
        return TypeInformation.of(new TypeHint<Tuple8<Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 9:
        return TypeInformation.of(new TypeHint<Tuple9<Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      case 10:
        return TypeInformation.of(new TypeHint<Tuple10<Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable, Comparable>>() {
        });
      default:
        throw new IllegalArgumentException("This arity is not supported");
    }
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

  public FactsSet evaluate(Program program, FactsSet facts, Set<String> relationsToOutput) {
    //Initialize globals
    cache = new HashMap<>();
    factsSet = facts;

    //We to program the loading from the factset
    facts.getRelations().forEach(relation -> program.addRule(buildLoadRuleForRelation(relation)));

    SimpleFactsSet result = new SimpleFactsSet();
    (new LogicalPlanBuilder(BUILDS_IN, relationsToOutput)).getPlanForProgram(program).entrySet().stream()
            .flatMap(relationPlan -> {
              String relation = relationPlan.getKey();
              PlanNode plan = optimize(relationPlan.getValue());
              LOGGER.info("Evaluating relation " + relation + " with plan:\n" + plan.toPrettyString());
              return stream(mapPlanNode(plan).map(dataSet ->
                      (DataSet<Tuple2<String, Comparable[]>>) dataSet.map((MapFunction<Tuple, Tuple2<String, Comparable[]>>) row -> {
                        Comparable[] args = new Comparable[row.getArity()];
                        for (int i = 0; i < args.length; i++) {
                          args[i] = row.getField(i);
                        }
                        return new Tuple2<>(relation, args);
                      }).returns(new TypeHint<Tuple2<String, Comparable[]>>() {
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

  private Optional<DataSet<Tuple>> mapPlanNode(PlanNode planNode) {
    if (!cache.containsKey(planNode)) {
      cache.put(planNode, buildForPlanNode(planNode));
    }
    return cache.get(planNode);
  }

  private Optional<DataSet<Tuple>> buildForPlanNode(PlanNode node) {
    if (node instanceof BuiltinNode) {
      return mapBuiltinNode((BuiltinNode) node);
    } else if (node instanceof ConstantEqualityFilterNode) {
      return mapConstantEqualityFilterNode((ConstantEqualityFilterNode) node);
    } else if (node instanceof JoinNode) {
      return mapJoinNode((JoinNode) node);
    } else if (node instanceof AntiJoinNode) {
      return mapAntiJoinNode((AntiJoinNode) node);
    } else if (node instanceof ProjectNode) {
      return mapProjectNode((ProjectNode) node);
    } else if (node instanceof RecursionNode) {
      return mapRecursionNode((RecursionNode) node);
    } else if (node instanceof UnionNode) {
      return mapUnionNode((UnionNode) node);
    } else if (node instanceof VariableEqualityFilterNode) {
      return mapVariableEqualityFilterNode((VariableEqualityFilterNode) node);
    } else if (node instanceof FactNode) {
      return mapFactNode((FactNode) node);
    } else {
      throw new IllegalArgumentException("Unknown node type: " + node.toString());
    }
  }

  private Optional<DataSet<Tuple>> mapBuiltinNode(BuiltinNode node) {
    String name = node.compoundTerm.name;
    switch (name) {
      case "flink_entry_values":
        String relation = ((String) ((Constant) node.compoundTerm.args[0]).getValue()).trim();
        //TODO: do not materialize here
        List<Tuple> tuples = factsSet.getByRelation(relation).map(FlinkEvaluator::newTuple).collect(Collectors.toList());
        if (tuples.isEmpty()) {
          LOGGER.info("Empty relation: " + relation);
          return Optional.empty();
        } else {
          return Optional.of(env.fromCollection(tuples, typeInfo(node.getArity())));
        }
      case "bash_command":
        String command = ((String) ((Constant) node.compoundTerm.args[0]).getValue()).trim();
        if (command.startsWith("cat")) {
          Path file = Paths.get(command.substring(4).trim()).toAbsolutePath();
          Pattern pattern = Pattern.compile("[ \t\r\n]");
          return Optional.of(env.readTextFile("file://" + file.toString()).map(line ->
                  newTuple(Arrays.stream(pattern.split(line)).toArray(Comparable[]::new))
          ).returns(typeInfo(node.getArity())));
        } else {
          throw new IllegalArgumentException("Unsupported bash command: " + node.toString());
        }
      default:
        throw new IllegalArgumentException("Unsupported build-in: " + node.toString());
    }
  }

  private Optional<DataSet<Tuple>> mapConstantEqualityFilterNode(ConstantEqualityFilterNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int field = node.getField();
      Comparable value = node.getValue();
      return dataSet.filter(row -> value.equals(row.getField(field)));
    });
  }

  private Optional<DataSet<Tuple>> mapJoinNode(JoinNode node) {
    return mapPlanNode(node.getLeft()).flatMap(left ->
            mapPlanNode(node.getRight()).map(right ->
                    left.join(right)
                            .where(node.getLeftProjection())
                            .equalTo(node.getRightProjection())
                            .with(FlinkEvaluator::concatTuples)
                            .returns(typeInfo(node.getArity()))

            )
    );
  }

  private Optional<DataSet<Tuple>> mapAntiJoinNode(AntiJoinNode node) {
    return mapPlanNode(node.getLeft()).flatMap(left ->
            mapPlanNode(node.getRight()).map(right ->
                    left.coGroup(right)
                            .where(node.getLeftProjection())
                            .equalTo("*")
                            .with((left1, right1, collector) -> {
                              if (!right1.iterator().hasNext()) {
                                left1.forEach(collector::collect);
                              }
                            })
                            .returns(typeInfo(node.getArity()))

            )
    );
  }

  private Optional<DataSet<Tuple>> mapProjectNode(ProjectNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int[] projection = node.getProjection();
      Comparable[] constants = node.getConstants();
      return dataSet.map(row -> {
        int arity = projection.length;
        Tuple result = newTuple(arity);
        for (int i = 0; i < arity; i++) {
          if (projection[i] >= 0) {
            result.setField(row.getField(projection[i]), i);
          } else if (constants.length > i) {
            result.setField(constants[i], i);
          }
        }
        return result;
      }).returns(typeInfo(node.getArity()));
    });
  }

  private Optional<DataSet<Tuple>> mapRecursionNode(RecursionNode node) {
    return mapPlanNode(node.getExitPlan()).map(initialSet -> {
      DeltaIteration<Tuple, Tuple> iteration = initialSet.iterateDelta(initialSet, MAX_ITERATION, range(node.getArity()));
      cache.put(node.getDelta(), Optional.of(iteration.getWorkset()));
      cache.put(node.getFull(), Optional.of(iteration.getSolutionSet()));
      return mapPlanNode(node.getRecursivePlan())
              .map(recursivePlan -> iteration.closeWith(recursivePlan, recursivePlan))
              .orElse(initialSet);
    });
  }

  private Optional<DataSet<Tuple>> mapFactNode(FactNode node) {
    List<Tuple> tuples = node.getFacts().stream().map(FlinkEvaluator::newTuple).collect(Collectors.toList());
    if (tuples.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(env.fromCollection(tuples, typeInfo(node.getArity())));
    }
  }

  private int[] range(int bound) {
    return IntStream.range(0, bound).toArray();
  }

  private Optional<DataSet<Tuple>> mapUnionNode(UnionNode node) {
    return node.getChildren().stream()
            .flatMap(child -> stream(this.mapPlanNode(child)))
            .reduce(DataSet::union);
  }

  private Optional<DataSet<Tuple>> mapVariableEqualityFilterNode(VariableEqualityFilterNode node) {
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
