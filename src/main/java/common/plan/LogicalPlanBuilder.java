package common.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.Program;
import common.parser.Rule;
import common.parser.Term;
import common.parser.Variable;

/**
 * TODO: support recursion f(x,z) <- f(x,y), f(y,z). (join with full)
 */
public class LogicalPlanBuilder {

  Set<String> builtin = new HashSet<>(), tables = new HashSet<>();

  private Map<RelationWithDeltaNodes, PlanNode> planForRelation = new HashMap<>();

  private Map<String, List<Rule>> rulesByHeadRelation = new HashMap<>();

  public LogicalPlanBuilder() {
  }

  public LogicalPlanBuilder(Set<String> builtin, Set<String> tables) {
    this.builtin = builtin;
    this.tables = tables;
  }

  private static <T, U> Map<T, U> withEntry(Map<T, U> map, T key, U value) {
    Map<T, U> clone = new HashMap<>();
    clone.putAll(map);
    clone.put(key, value);
    return clone;
  }

  public static int[] concat(int[] first, int[] second) {
    int[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public Map<String, PlanNode> getPlanForProgram(Program program) {
    Set<String> relations = new HashSet<>();
    program.rules.forEach(rule -> {
      rulesByHeadRelation.computeIfAbsent(rule.head.signature(), (k) -> new ArrayList<>()).add(rule);
      relations.add(rule.head.signature());
      for (CompoundTerm ct : rule.body) {
        if (!builtin.contains(ct.name)) {
          relations.add(ct.signature());
        }
      }
    });

    Map<String, PlanNode> planNodes = new HashMap<>();
    relations.forEach(relation -> planNodes.put(relation, getPlanForRelation(relation, Collections.emptyMap()).simplify()));
    return planNodes;
  }

  private PlanNode createTableNode(String relation, PlanNode prev) {
    int pos = relation.lastIndexOf('/');
    PlanNode r = new TableNode(relation, Integer.parseInt(relation.substring(pos + 1)));
    if (tables.contains(relation)) {
      return prev == null ? r : prev.union(r);
    } else {
      // TODO: deal with case prev == null
      return prev == null ? r : prev;
    }
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
    return planForRelation.computeIfAbsent(relationWithDeltaNodes, (rel) -> {
      //We split rules between exit ones and recursive ones
      List<Rule> exitRules = new ArrayList<>();
      List<Rule> recursiveRules = new ArrayList<>();
      rulesByHeadRelation.getOrDefault(relation, Collections.emptyList()).forEach(rule -> {
        if (this.isRecursive(rule)) {
          recursiveRules.add(rule);
        } else {
          exitRules.add(rule);
        }
      });

      //We map exit rules
      PlanNode exitPlan = exitRules.stream().map(rule -> getPlanForRule(rule, filteredDeltaNode)).reduce(UnionNode::new)
          .map(node -> createTableNode(relation, node)).orElse(createTableNode(relation, null));

      if (recursiveRules.isEmpty()) {
        return exitPlan; //No recursion
      }

      //We build recursion
      RecursionNode recursionPlan = exitPlan.recursion();
      Map<String, PlanNode> newDeltaNodes = withEntry(filteredDeltaNode, relation, recursionPlan.getDelta());
      recursiveRules.forEach(rule -> {
        recursionPlan.addRecursivePlan(getPlanForRule(rule, newDeltaNodes)); //TODO: update
      });
      return recursionPlan;
    });
  }

  private PlanNode getPlanForRule(Rule rule, Map<String, PlanNode> deltaNodes) {
    //TODO: check if the rule is sane
    //a(X,Y,c) <- b(X,d), c(X,Y)
    //b.filter(1, d).map([0, -1]).join(c.map([0, 1])).project([0, 1], [null, null, c])

    //Variables integer encoding
    Map<Term, Integer> variablesEncoding = new HashMap<>();
    Map<Term, Integer> variablesCount = new HashMap<>();
    Stream.concat(rule.head.getVariables(), rule.body.stream().flatMap(tuple -> Arrays.stream(tuple.args)).flatMap(t -> t.getVariables())) //
        .forEach(arg -> {
          if (arg instanceof Variable) {
            variablesEncoding.computeIfAbsent(arg, (key) -> variablesEncoding.size());
            variablesCount.merge(arg, 1, (a, b) -> a + b);
          }
        });

    NodeWithMask body = rule.body.stream().map(term -> {

      CompoundTerm ct = ((CompoundTerm) term);
      int[] colToVar = new int[term.args.length];
      int[] varToCol = new int[variablesEncoding.size()];
      Arrays.fill(colToVar, -1);
      Arrays.fill(varToCol, -1);

      PlanNode termNode;
      if (builtin.contains(ct.name)) {
        termNode = new BuiltinNode(ct);
        List<Variable> v = ct.getVariables().collect(Collectors.toList());
        colToVar = new int[v.size()];
        for (int i = 0; i < v.size(); i++) {
          colToVar[i] = variablesEncoding.get(v.get(i));
          varToCol[colToVar[i]] = i;
        }
      } else {
        termNode = getPlanForRelation(ct.signature(), deltaNodes);
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
      int size = Math.max(nm1.node.arity(), nm2.node.arity());
      int[] colLeft = new int[size];
      int[] colRight = new int[size];

      int count = 0;
      boolean[] done = new boolean[nm2.colToVar.length];
      for (int i = 0; i < nm1.colToVar.length; i++) {
        int col2 = nm2.varToCol[nm1.colToVar[i]];
        if (col2 >= 0) {
          colLeft[count] = i;
          colRight[count++] = col2;
          done[col2] = true;
        }
      }
      for (int i = 0; i < nm2.colToVar.length; i++) {
        if (done[i]) continue;
        int var = nm2.colToVar[i];
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
      JoinNode jn = new JoinNode(nm1.node, nm2.node, colLeft, colRight);
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
    return hasAncestor(rule, rule.head.signature());
  }

  private boolean hasAncestor(String relation, String ancestor) {
    return rulesByHeadRelation.getOrDefault(relation, Collections.emptyList()).stream().anyMatch(rule -> this.hasAncestor(rule, ancestor));
  }

  private boolean hasAncestor(Rule rule, String ancestor) {
    return rule.body.stream().anyMatch(tuple -> tuple.signature().equals(ancestor) || hasAncestor(tuple.signature(), ancestor));
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
