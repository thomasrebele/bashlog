package common.plan;

import common.parser.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: support recursion f(x,z) <- f(x,y), f(y,z). (join with full)
 */
public class LogicalPlanBuilder {

  private static final PlanSimplifier SIMPLIFIER = new PlanSimplifier();

  private Set<String> builtin = new HashSet<>();
  private Set<String> relationsToOutput = new HashSet<>();

  private Map<RelationWithDeltaNodes, PlanNode> planForRelation = new HashMap<>();

  private Program program;

  public LogicalPlanBuilder() {
  }

  public LogicalPlanBuilder(Set<String> builtin, Set<String> relationsToOutput) {
    this.builtin = builtin;
    this.relationsToOutput = relationsToOutput;
  }

  private static <T, U> Map<T, U> withEntry(Map<T, U> map, T key, U value) {
    Map<T, U> clone = new HashMap<>();
    clone.putAll(map);
    clone.put(key, value);
    return clone;
  }

  private static int[] concat(int[] first, int[] second) {
    int[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public Map<String, PlanNode> getPlanForProgram(Program program) {
    this.program = program;
    //We fill relationsToOutput if needed
    if (relationsToOutput.isEmpty()) {
      program.rules().forEach(rule -> relationsToOutput.add(rule.head.getRelation()));
    }

    Map<String, PlanNode> planNodes = new HashMap<>();
    relationsToOutput.forEach(relation -> planNodes.put(relation, SIMPLIFIER.simplify(getPlanForRelation(relation, Collections.emptyMap()))));
    return planNodes;
  }

  private PlanNode getPlanForRelation(String relation, Map<String, PlanNode> deltaNodes) {
    //If we should use a delta node
    if (deltaNodes.containsKey(relation)) {
      return deltaNodes.get(relation);
    }

    //We filter the delta node map to remove not useful deltas
    Map<String, PlanNode> filteredDeltaNode = new HashMap<>();
    deltaNodes.forEach((key, value) -> {
      if (hasAncestor(relation, key)) {
        filteredDeltaNode.put(key, value);
      }
    });

    RelationWithDeltaNodes relationWithDeltaNodes = new RelationWithDeltaNodes(relation, filteredDeltaNode);
    if (!planForRelation.containsKey(relationWithDeltaNodes)) {
      //We split rules between exit ones and recursive ones
      List<Rule> exitRules = new ArrayList<>();
      List<Rule> recursiveRules = new ArrayList<>();
      program.rulesForRelation(relation).forEach(rule -> {
        if (isRecursive(rule)) {
          recursiveRules.add(rule);
        } else {
          exitRules.add(rule);
        }
      });

      //We map exit rules
      PlanNode exitPlan = exitRules.stream()
              .map(rule -> getPlanForRule(rule, filteredDeltaNode))
              .reduce(UnionNode::new)
              .orElseGet(() -> new UnionNode(Integer.parseInt(relation.split("/")[1])));

      if (recursiveRules.isEmpty()) {
        return exitPlan; //No recursion
      }

      //We build recursion
      RecursionNode recursionPlan = exitPlan.recursion();
      Map<String, PlanNode> newDeltaNodes = withEntry(filteredDeltaNode, relation, recursionPlan.getDelta());
      recursiveRules.forEach(rule -> {
        recursionPlan.addRecursivePlan(getPlanForRule(rule, newDeltaNodes)); //TODO: update
      });
      planForRelation.put(relationWithDeltaNodes, recursionPlan);
    }
    return planForRelation.get(relationWithDeltaNodes);
  }

  private PlanNode getPlanForRule(Rule rule, Map<String, PlanNode> deltaNodes) {
    //TODO: check if the rule is sane
    //a(X,Y,c) <- b(X,d), c(X,Y)
    //b.filter(1, d).map([0, -1]).join(c.map([0, 1])).project([0, 1], [null, null, c])

    //Variables integer encoding
    Map<Term, Integer> variablesEncoding = new HashMap<>();
    Stream.concat(
            rule.head.getVariables(),
            rule.body.stream().flatMap(tuple -> Arrays.stream(tuple.args)).flatMap(Term::getVariables)
    ).forEach(arg ->
            variablesEncoding.computeIfAbsent(arg, (key) -> variablesEncoding.size())
    );

    NodeWithMask body = rule.body.stream().map(term -> {
      int[] colToVar = new int[term.args.length];
      int[] varToCol = new int[variablesEncoding.size()];
      Arrays.fill(colToVar, -1);
      Arrays.fill(varToCol, -1);

      PlanNode termNode;
      if (builtin.contains(term.name)) {
        termNode = new BuiltinNode(term);
        List<Variable> v = term.getVariables().collect(Collectors.toList());
        colToVar = new int[v.size()];
        for (int i = 0; i < v.size(); i++) {
          colToVar[i] = variablesEncoding.get(v.get(i));
          varToCol[colToVar[i]] = i;
        }
      } else {
        termNode = getPlanForRelation(term.getRelation(), deltaNodes);
        for (int i = 0; i < term.args.length; i++) {
          Term arg = term.args[i];
          if (arg instanceof Variable) {
            colToVar[i] = variablesEncoding.get(arg);
            varToCol[colToVar[i]] = i;
          } else if (arg instanceof Constant) {
            termNode = termNode.equalityFilter(i, (Comparable) ((Constant) arg).getValue());
          } else {
            throw new UnsupportedOperationException("cannot handle " + termNode);
          }
        }
      }

      for (int i = 0; i < colToVar.length; i++) {
        int var = colToVar[i];
        if (var >= 0 && varToCol[var] != i) {
          termNode = termNode.equalityFilter(i, varToCol[var]);
        }
      }

      return new NodeWithMask(termNode, colToVar, varToCol);
    }).reduce((nm1, nm2) -> {
      int size = Math.max(nm1.node.getArity(), nm2.node.getArity());
      int[] colLeft = new int[size];
      int[] colRight = new int[size];

      int count = 0;
      boolean[] done = new boolean[nm2.colToVar.length];
      for (int i = 0; i < nm1.colToVar.length; i++) {
        int var = nm1.colToVar[i];
        if (var < 0) continue;
        int col2 = nm2.varToCol[var];
        if (col2 >= 0) {
          colLeft[count] = i;
          colRight[count++] = col2;
          done[col2] = true;
        }
      }
      for (int i = 0; i < nm2.colToVar.length; i++) {
        if (done[i]) continue;
        int var = nm2.colToVar[i];
        if (var < 0) continue;
        int col1 = nm1.varToCol[var];
        if (col1 < 0) {
          nm1.varToCol[var] = nm1.colToVar.length + i;
        }
        if (col1 >= 0) {
          colRight[count] = i;
          colLeft[count++] = col1;
        }
      }

      colLeft = Arrays.copyOfRange(colLeft, 0, count);
      colRight = Arrays.copyOfRange(colRight, 0, count);
      PlanNode jn = nm1.node.join(nm2.node, colLeft, colRight);
      return new NodeWithMask(jn, concat(nm1.colToVar, nm2.colToVar), nm1.varToCol);
    }).orElseThrow(() -> new UnsupportedOperationException("rule without body"));

    if (builtin.contains(rule.head.name)) {
      return body.node;
    }

    int[] projection = new int[rule.head.args.length];
    Arrays.fill(projection, -1);
    Comparable[] resultConstants = new Comparable[rule.head.args.length];
    for (int i = 0; i < rule.head.args.length; i++) {
      Term t = rule.head.args[i];
      if (t instanceof Variable) {
        projection[i] = body.varToCol[variablesEncoding.get(t)];
      } else if (t instanceof Constant) {
        // TODO: this is an unchecked constraint!
        resultConstants[i] = (Comparable) ((Constant) t).getValue();
      } else {
        throw new UnsupportedOperationException();
      }
    }
    return body.node.project(projection, resultConstants);
  }

  private boolean isRecursive(Rule rule) {
    return program.getDependencies(rule).contains(rule.head.getRelation());
  }

  private boolean hasAncestor(String relation, String ancestor) {
    return program.getDependencies(relation).contains(ancestor);
  }

  private static class NodeWithMask {

    private PlanNode node;

    private int[] colToVar, varToCol;

    NodeWithMask(PlanNode node, int[] colToVar, int[] varToCol) {
      this.node = node;
      this.colToVar = colToVar;
      this.varToCol = varToCol;
    }
  }

  private static class RelationWithDeltaNodes {

    private String relation;

    private Map<String, PlanNode> deltaNodes;

    RelationWithDeltaNodes(String relation, Map<String, PlanNode> deltaNodes) {
      this.relation = relation;
      this.deltaNodes = deltaNodes;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof RelationWithDeltaNodes && relation.equals(((RelationWithDeltaNodes) obj).relation)
          && deltaNodes.equals(((RelationWithDeltaNodes) obj).deltaNodes);
    }

    @Override
    public int hashCode() {
      return relation.hashCode();
    }
  }
}
