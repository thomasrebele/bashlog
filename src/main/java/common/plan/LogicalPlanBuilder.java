package common.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.Program;
import common.parser.Rule;
import common.parser.Term;
import common.parser.Variable;

public class LogicalPlanBuilder {

  private Map<String, PlanNode> planForRelation = new HashMap<>();

  private Map<String, List<Rule>> rulesByHeadRelation = new HashMap<>();

  public Map<String, PlanNode> getPlanForProgram(Program program, Set<String> builtin, Set<String> tables) {
    Set<String> relations = new HashSet<>();
    program.rules.forEach(rule -> {
      rulesByHeadRelation.computeIfAbsent(rule.head.signature(), (k) -> new ArrayList<>()).add(rule);
      relations.add(rule.head.signature());
      for (CompoundTerm ct : rule.body) {
        relations.add(ct.signature());
      }
    });

    Map<String, PlanNode> planNodes = new HashMap<>();
    relations.forEach(relation -> planNodes.put(relation, getPlanForRelation(relation, builtin, tables)));
    return planNodes;
  }

  static final Pattern REMOVE_ARITY = Pattern.compile("\\/.*");

  private PlanNode createTableNode(PlanNode prev, String relation, Set<String> tables) {
    int pos = relation.lastIndexOf('/');
    PlanNode r = new TableNode(relation, Integer.parseInt(relation.substring(pos + 1)));
    if (tables.contains(relation)) {
      return prev == null ? r : prev.union(r);
    } else {
      return prev == null ? r : prev;
    }
  }

  private PlanNode getPlanForRelation(String relation, Set<String> builtin, Set<String> tables) {
    return planForRelation.computeIfAbsent(relation,
        (rel) -> rulesByHeadRelation.getOrDefault(rel, Collections.emptyList()).stream() //
            .map(r -> getPlanForRule(r, builtin, tables)).reduce(UnionNode::new) //
            .map(node -> createTableNode(node, relation, tables)).orElse(createTableNode(null, relation, tables)));
  }

  public static int[] concat(int[] first, int[] second) {
    int[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  private PlanNode getPlanForRule(Rule rule, Set<String> builtin, Set<String> tables) throws Error {
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

    Stream<Variable> vx;

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
        termNode = getPlanForRelation(ct.signature(), builtin, tables);
        for (int i = 0; i < term.args.length; i++) {
          Term arg = term.args[i];
          if (arg instanceof Variable) {
            colToVar[i] = variablesEncoding.get(arg);
            varToCol[colToVar[i]] = i;
          } else if (arg instanceof Constant) {
            termNode = termNode.equalityFilter(i, ((Constant) arg).value);
          } else {

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
    for (int i = 0; i < rule.head.args.length; i++) {
      // TODO selection
      Term t = rule.head.args[i];
      if (t instanceof Variable) {

        projection[i] = body.varToCol[variablesEncoding.get(t)];
      }
    }
    return body.node.project(projection);

    //Body tuples filter and map
    /*PlanNode body = rule.body.stream().map(tuple -> {
      PlanNode tupleNode = getPlanForTerm(tuple, builtin, tables);
    
      int[] projection = new int[variablesEncoding.size()];
      Arrays.fill(projection, -1);
      boolean[] mask = new boolean[variablesEncoding.size()];
    
      if (builtin.contains(tuple.name)) {
        Arrays.fill(mask, true);
        return new NodeWithMask(tupleNode, mask);
      }
    
      Map<Term, Integer> seenVariables = new HashMap<>();
      for (int i = 0; i < tuple.args.length; i++) {
        Term arg = tuple.args[i];
        if (arg instanceof Variable) {
          if (!builtin.contains(tuple.name) && seenVariables.containsKey(arg)) { //Already in a tuple, we are in a tuple(X,X) case
            tupleNode = tupleNode.equalityFilter(seenVariables.get(arg).intValue(), i);
          }
          projection[variablesEncoding.get(arg)] = i;
          mask[variablesEncoding.get(arg)] = true;
          seenVariables.put(arg, i);
        } else {
          if (!builtin.contains(tuple.name)) tupleNode = tupleNode.equalityFilter(i, arg);
        }
      }
    
      return new NodeWithMask(tupleNode.project(projection), mask);
    }).reduce((nm1, nm2) -> {
      boolean[] intersectionMask = new boolean[nm1.mask.length];
      boolean[] unionMask = new boolean[nm1.mask.length];
      for (int i = 0; i < nm2.mask.length; i++) {
        intersectionMask[i] = nm1.mask[i] && nm2.mask[i];
        unionMask[i] = nm1.mask[i] || nm2.mask[i];
      }
      return new NodeWithMask(new JoinNode(nm1.node, nm2.node, intersectionMask), unionMask);
    }).map(nm -> nm.node).orElseThrow(() -> new UnsupportedOperationException("Rule without body"));
    
    int[] resultProjection = new int[rule.head.args.length];
    Arrays.fill(resultProjection, -1);
    Object[] resultConstants = new Object[rule.head.args.length];
    for (int i = 0; i < rule.head.args.length; i++) {
      Term arg = rule.head.args[i];
      if (arg instanceof Variable) {
        resultProjection[i] = variablesEncoding.get(arg);
      } else {
        resultConstants[i] = arg;
      }
    }
    return body.project(resultProjection, resultConstants);*/
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
}
