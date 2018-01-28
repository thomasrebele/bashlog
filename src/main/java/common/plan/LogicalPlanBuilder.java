package common.plan;

import common.parser.*;
import common.plan.node.BuiltinNode;
import common.plan.node.JoinNode;
import common.plan.node.PlanNode;
import common.plan.node.RecursionNode;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogicalPlanBuilder {
  private Set<String> builtin;
  private Set<String> relationsToOutput;
  private Map<RelationWithDeltaNodes, PlanNode> planForRelation;
  private Program program;

  public LogicalPlanBuilder(Set<String> builtin, Set<String> relationsToOutput) {
    this.builtin = builtin;
    this.relationsToOutput = relationsToOutput;
  }

  public LogicalPlanBuilder(Set<String> builtin) {
    this(builtin, new HashSet<>());
  }

  private static <T, U> Map<T, U> withEntry(Map<T, U> map, T key, U value) {
    Map<T, U> clone = new HashMap<>();
    clone.putAll(map);
    clone.put(key, value);
    return clone;
  }

  public Map<String, PlanNode> getPlanForProgram(Program program) {
    planForRelation = new HashMap<>();
    this.program = program;

    //We fill relationsToOutput if needed
    if (relationsToOutput.isEmpty()) {
      program.rules().forEach(rule -> relationsToOutput.add(rule.head.getRelation()));
    }

    Map<String, PlanNode> planNodes = new HashMap<>();
    relationsToOutput.forEach(relation ->
            planNodes.put(relation, getPlanForRelation(relation, Collections.emptyMap()))
    );
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
      if (program.hasAncestor(relation, key)) {
        filteredDeltaNode.put(key, value);
      }
    });

    RelationWithDeltaNodes relationWithDeltaNodes = new RelationWithDeltaNodes(relation, filteredDeltaNode);
    if (!planForRelation.containsKey(relationWithDeltaNodes)) {
      //We split rules between exit ones and recursive ones
      List<Rule> exitRules = new ArrayList<>();
      List<Rule> recursiveRules = new ArrayList<>();
      program.rulesForRelation(relation).forEach(rule -> {
        if (program.isRecursive(rule, deltaNodes.keySet())) { //We do not want to tag as recursive rules that are already in a loop
          recursiveRules.add(rule);
        } else {
          exitRules.add(rule);
        }
      });

      //We map exit rules
      PlanNode exitPlan = exitRules.stream()
              .map(rule -> getPlanForRule(rule, filteredDeltaNode))
              .reduce(PlanNode::union)
              .orElseGet(() -> PlanNode.empty(Integer.parseInt(relation.split("/")[1])));

      if (recursiveRules.isEmpty()) {
        return exitPlan; //No recursion
      }

      //We build recursion
      RecursionNode recursionPlan = exitPlan.recursion();
      Map<String, PlanNode> newDeltaNodes = withEntry(filteredDeltaNode, relation, recursionPlan.getDelta());
      recursiveRules.forEach(rule ->
              recursionPlan.addRecursivePlan(introduceFullRecursion(getPlanForRule(rule, newDeltaNodes), recursionPlan.getDelta(), recursionPlan.getFull()))
      );
      planForRelation.put(relationWithDeltaNodes, recursionPlan);
    }
    return planForRelation.get(relationWithDeltaNodes);
  }

  private PlanNode introduceFullRecursion(PlanNode baseNode, PlanNode delta, PlanNode full) {
    return baseNode.transform(n -> {
      //We look for joins between to subtrees depending on dela and we replace one by Full
      if (n instanceof JoinNode) {
        JoinNode join = (JoinNode) n;
        if (join.getLeft().contains(delta) && join.getRight().contains(delta)) {
          return join.getLeft().join(join.getRight().replace(delta, full), join.getLeftProjection(), join.getRightProjection()).union(
                  join.getLeft().replace(delta, full).join(join.getRight(), join.getLeftProjection(), join.getRightProjection())
          );
        }
      }
      return n;
    });
  }

  private PlanNode getPlanForRule(Rule rule, Map<String, PlanNode> deltaNodes) {
    //TODO: check if the rule is sane
    //Variables integer encoding
    Map<Term, Integer> variablesEncoding = new HashMap<>();
    Stream.concat(
            rule.head.getVariables(),
            rule.body.stream().flatMap(tuple -> Arrays.stream(tuple.args)).flatMap(Term::getVariables)
    ).forEach(arg ->
            variablesEncoding.computeIfAbsent(arg, (key) -> variablesEncoding.size())
    );

    List<NodeWithMask> positiveNodes = new ArrayList<>();
    List<NodeWithMask> negativeNodes = new ArrayList<>();
    rule.body.forEach(term -> {
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
            termNode = termNode.equalityFilter(i, ((Constant<?>) arg).getValue());
          } else {
            throw new UnsupportedOperationException("cannot handle " + term);
          }
        }
      }

      for (int i = 0; i < colToVar.length; i++) {
        int var = colToVar[i];
        if (var >= 0 && varToCol[var] != i) {
          termNode = termNode.equalityFilter(i, varToCol[var]);
        }
      }

      if (term.negated) {
        negativeNodes.add(new NodeWithMask(termNode, colToVar, varToCol));
      } else {
        positiveNodes.add(new NodeWithMask(termNode, colToVar, varToCol));
      }
    });

    NodeWithMask positive = positiveNodes.stream()
            .reduce((nm1, nm2) -> group(nm1, nm2, false))
            .orElseThrow(() -> new UnsupportedOperationException("rule without positive body: " + rule.toString()));
    NodeWithMask body = negativeNodes.stream().reduce((nm1, nm2) -> group(nm1, nm2, false))
            .map(negations -> group(positive, negations, true))
            .orElse(positive);

    if (builtin.contains(rule.head.name)) {
      return body.node;
    }

    int[] projection = new int[rule.head.args.length];
    Arrays.fill(projection, -1);
    Comparable<?>[] resultConstants = new Comparable[rule.head.args.length];
    for (int i = 0; i < rule.head.args.length; i++) {
      Term t = rule.head.args[i];
      if (t instanceof Variable) {
        projection[i] = body.varToCol[variablesEncoding.get(t)];
      } else if (t instanceof Constant) {
        // TODO: this is an unchecked constraint!
        resultConstants[i] = ((Constant<?>) t).getValue();
      } else {
        throw new UnsupportedOperationException();
      }
    }
    return body.node.project(projection, resultConstants);
  }

  private NodeWithMask group(NodeWithMask nmLeft, NodeWithMask nmRight, boolean antiJoin) {
    int size = Math.max(nmLeft.node.getArity(), nmRight.node.getArity());
    int[] joinProjectionLeft = new int[size];
    int[] joinProjectionRight = new int[size];

    int count = 0;
    boolean[] done = new boolean[nmRight.colToVar.length];
    for (int i = 0; i < nmLeft.colToVar.length; i++) {
      int var = nmLeft.colToVar[i];
      if (var < 0) {
        continue;
      }
      int col2 = nmRight.varToCol[var];
      if (col2 >= 0) {
        joinProjectionLeft[count] = i;
        joinProjectionRight[count] = col2;
        count++;
        done[col2] = true;
      }
    }
    for (int i = 0; i < nmRight.colToVar.length; i++) {
      if (done[i]) {
        continue;
      }
      int var = nmRight.colToVar[i];
      if (var < 0) {
        continue;
      }
      int col1 = nmLeft.varToCol[var];
      if (col1 >= 0) {
        joinProjectionRight[count] = i;
        joinProjectionLeft[count] = col1;
        count++;
      }
    }
    joinProjectionLeft = Arrays.copyOfRange(joinProjectionLeft, 0, count);
    joinProjectionRight = Arrays.copyOfRange(joinProjectionRight, 0, count);

    //Compute final projection that drops duplicates variables
    int maxArity = nmLeft.node.getArity() + nmRight.node.getArity();
    int[] finalProjection = new int[maxArity];
    int[] finalVarToCol = new int[nmLeft.varToCol.length];
    int[] finalColToVar = new int[maxArity];
    Arrays.fill(finalColToVar, -1);
    Arrays.fill(finalVarToCol, -1);
    int finalCount = 0;
    for (int i = 0; i < nmLeft.colToVar.length; i++) {
      int var = nmLeft.colToVar[i];
      if (var < 0) {
        continue;
      }
      if (finalVarToCol[var] < 0) {
        finalProjection[finalCount] = i;
        finalColToVar[finalCount] = var;
        finalVarToCol[var] = finalCount;
        finalCount++;
      }
    }
    if (!antiJoin) {
      for (int i = 0; i < nmRight.colToVar.length; i++) {
        int var = nmRight.colToVar[i];
        if (var < 0) {
          continue;
        }
        if (finalVarToCol[var] < 0) {
          finalProjection[finalCount] = i + nmLeft.node.getArity();
          finalColToVar[finalCount] = var;
          finalVarToCol[var] = finalCount;
          finalCount++;
        }
      }
    }
    finalProjection = Arrays.copyOfRange(finalProjection, 0, finalCount);
    finalColToVar = Arrays.copyOfRange(finalColToVar, 0, finalCount);

    PlanNode jn = antiJoin
            ? nmLeft.node.antiJoin(nmRight.node.project(joinProjectionRight), joinProjectionLeft)
            : nmLeft.node.join(nmRight.node, joinProjectionLeft, joinProjectionRight);
    return new NodeWithMask(jn.project(finalProjection), finalColToVar, finalVarToCol);
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
