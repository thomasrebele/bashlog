package common.plan;

import common.parser.*;

import java.util.*;

/**
 * TODO: support recursion f(x,z) <- f(x,y), f(y,z). (join with full)
 */
public class LogicalPlanBuilder {

  private Map<RelationWithDeltaNodes, PlanNode> planForRelation = new HashMap<>();

  private Map<String, List<Rule>> rulesByHeadRelation = new HashMap<>();

  private static <T, U> Map<T, U> withEntry(Map<T, U> map, T key, U value) {
    Map<T, U> clone = new HashMap<>();
    clone.putAll(map);
    clone.put(key, value);
    return clone;
  }

  public Map<String, PlanNode> getPlanForProgram(Program program) {
    Set<String> relations = new HashSet<>();
    program.rules.forEach(rule -> {
      rulesByHeadRelation.computeIfAbsent(rule.head.signature(), (k) -> new ArrayList<>()).add(rule);
      relations.add(rule.head.signature());
      for (CompoundTerm ct : rule.body) {
        relations.add(ct.signature());
      }
    });

    Map<String, PlanNode> planNodes = new HashMap<>();
    relations.forEach(relation -> planNodes.put(relation, getPlanForRelation(relation, Collections.emptyMap())));
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
      PlanNode exitPlan = exitRules.stream()
              .map(rule -> getPlanForRule(rule, filteredDeltaNode))
              .reduce(UnionNode::new)
              .map(node -> node.union(new TableNode(relation)))
              .orElse(new TableNode(relation));

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
    rule.body.forEach(tuple -> Arrays.stream(tuple.args).forEach(arg -> {
      if (arg instanceof Variable) {
        variablesEncoding.computeIfAbsent(arg, (key) -> variablesEncoding.size());
      }
    }));

    //Body tuples filter and map
    PlanNode body = rule.body.stream().map(tuple -> {
      PlanNode tupleNode = getPlanForRelation(tuple.signature(), deltaNodes);

      int[] projection = new int[variablesEncoding.size()];
      Arrays.fill(projection, -1);
      boolean[] mask = new boolean[variablesEncoding.size()];

      Map<Term, Integer> seenVariables = new HashMap<>();
      for (int i = 0; i < tuple.args.length; i++) {
        Term arg = tuple.args[i];
        if (arg instanceof Variable) {
          if (seenVariables.containsKey(arg)) { //Already in a tuple, we are in a tuple(X,X) case
            tupleNode = tupleNode.equalityFilter(seenVariables.get(arg).intValue(), i);
          }
          projection[variablesEncoding.get(arg)] = i;
          mask[variablesEncoding.get(arg)] = true;
          seenVariables.put(arg, i);
        } else {
          tupleNode = tupleNode.equalityFilter(i, arg);
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
    Comparable[] resultConstants = new Comparable[rule.head.args.length];
    for (int i = 0; i < rule.head.args.length; i++) {
      Term arg = rule.head.args[i];
      if (arg instanceof Variable) {
        resultProjection[i] = variablesEncoding.get(arg);
      } else {
        resultConstants[i] = arg;
      }
    }
    return body.project(resultProjection, resultConstants);
  }

  private boolean isRecursive(Rule rule) {
    return hasAncestor(rule, rule.head.signature());
  }

  private boolean hasAncestor(String relation, String ancestor) {
    return rulesByHeadRelation.getOrDefault(relation, Collections.emptyList()).stream()
            .anyMatch(rule -> this.hasAncestor(rule, ancestor));
  }

  private boolean hasAncestor(Rule rule, String ancestor) {
    return rule.body.stream().anyMatch(tuple -> tuple.signature().equals(ancestor) || hasAncestor(tuple.signature(), ancestor));
  }

  private static class NodeWithMask {

    private PlanNode node;

    private boolean[] mask;

    NodeWithMask(PlanNode node, boolean[] mask) {
      this.node = node;
      this.mask = mask;
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
      return obj instanceof RelationWithDeltaNodes && relation.equals(((RelationWithDeltaNodes) obj).relation) && deltaNodes.equals(((RelationWithDeltaNodes) obj).deltaNodes);
    }

    @Override
    public int hashCode() {
      return relation.hashCode();
    }
  }
}
