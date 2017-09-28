package flinklog;

import common.parser.*;
import common.plan.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Evaluates a Program on a FactsSet and returns a new FactsSet
 * See flinklog.IntegrationTests for examples of programs.
 */
public class FlinkEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEvaluator.class);
  private static final int MAX_ITERATION = Integer.MAX_VALUE;

  private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
  private FactsSet factsSet;
  private Map<PlanNode, Optional<DataSet<FlinkRow>>> cache = new HashMap<>();

  public FlinkEvaluator() {
    env.registerType(Atom.class);
    env.registerType(Constant.class);
    env.registerType(Variable.class);
    env.getConfig().enableObjectReuse();
  }

  private static <T> Stream<T> stream(Optional<T> optional) {
    //TODO: remove when targeting JDK 9+
    return optional.map(Stream::of).orElse(Stream.empty());
  }

  public FactsSet evaluate(Program program, FactsSet facts) {
    //Initialize globals
    factsSet = facts;
    Pattern stripRelation = Pattern.compile("/\\d+$");

    SimpleFactsSet result = new SimpleFactsSet();
    (new LogicalPlanBuilder()).getPlanForProgram(program).entrySet().stream()
            .flatMap(relationPlan -> {
              String relation = relationPlan.getKey();
              return stream(this.mapPlanNode(relationPlan.getValue()).map(dataSet ->
                      (DataSet<CompoundTerm>) dataSet.map(row -> {
                        Term[] args = new Term[row.getArity()];
                        for (int i = 0; i < args.length; i++) {
                          args[i] = (Term) row.getField(i);
                        }
                        return new CompoundTerm(stripRelation.matcher(relation).replaceAll(""), args);
                      })
              ));
            })
            .reduce(DataSet::union)
            .ifPresent(dataSet -> {
              try {
                dataSet.collect().forEach(result::add);
              } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
              }
            });
    return result;
  }

  private Optional<DataSet<FlinkRow>> mapPlanNode(PlanNode planNode) {
    return cache.computeIfAbsent(planNode, (node) -> {
      if (node instanceof ConstantEqualityFilterNode) {
        return mapConstantEqualityFilterNode((ConstantEqualityFilterNode) node);
      } else if (node instanceof JoinNode) {
        return mapJoinNode((JoinNode) node);
      } else if (node instanceof ProjectNode) {
        return mapProjectNode((ProjectNode) node);
      } else if (node instanceof RecursionNode) {
        return mapRecursionNode((RecursionNode) node);
      } else if (node instanceof TableNode) {
        return mapTableNode((TableNode) node);
      } else if (node instanceof UnionNode) {
        return mapUnionNode((UnionNode) node);
      } else if (node instanceof VariableEqualityFilterNode) {
        return mapVariableEqualityFilterNode((VariableEqualityFilterNode) node);
      } else {
        throw new IllegalArgumentException("Unknown node type: " + node.toString());
      }
    });
  }

  private Optional<DataSet<FlinkRow>> mapConstantEqualityFilterNode(ConstantEqualityFilterNode node) {
    return mapPlanNode(node.getTable()).map(dataSet -> {
      int field = node.getField();
      Comparable value = node.getValue();
      return dataSet.filter(row -> value.equals(row.getField(field)));
    });
  }

  private KeySelector<FlinkRow, FlinkRow> selectorForColumns(int columns[]) {
    return new KeySelector<FlinkRow, FlinkRow>() {

      @Override
      public FlinkRow getKey(FlinkRow full) throws Exception {
        FlinkRow filtered = new FlinkRow(full.getArity());
        for (int i = 0; i < columns.length; i++) {
          filtered.setField(i, full.getField(columns[i]));
        }
        return filtered;
      }
    };
  }

  private Optional<DataSet<FlinkRow>> mapJoinNode(JoinNode node) {
    return mapPlanNode(node.getLeft()).flatMap(left -> mapPlanNode(node.getRight()).map(right -> {
      return left.join(right).where(selectorForColumns(node.colLeft)).equalTo(selectorForColumns(node.colRight)).with((leftRow, rightRow) -> {
        return FlinkRow.concat(leftRow, rightRow);
      });
    }));
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

  private Optional<DataSet<FlinkRow>> mapTableNode(TableNode node) {
    //TODO: do not materialize here
    List<FlinkRow> tuples = factsSet.getByRelation(node.getName()).map(tuple -> {
      FlinkRow row = new FlinkRow(tuple.length);
      for (int i = 0; i < tuple.length; i++) {
        row.setField(i, tuple[i]);
      }
      return row;
    }).collect(Collectors.toList());
    if (tuples.isEmpty()) {
      LOGGER.info("Empty relation: " + node.getName());
      return Optional.empty();
    } else {
      return Optional.of(env.fromCollection(tuples));
    }
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
