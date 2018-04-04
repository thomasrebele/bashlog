package rdf;

import common.parser.*;
import org.apache.commons.rdf.rdf4j.RDF4J;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.*;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SPARQLConverter {

  private static final UUID SALT = UUID.randomUUID();
  private static final Constant<Comparable<?>> NULL = new Constant<>(null);
  private static final List<QueryOptimizer> OPTIMIZERS = Arrays.asList(
          new BindingAssigner(),
          new CompareOptimizer(),
          new ConjunctiveConstraintSplitter(),
          new DisjunctiveConstraintOptimizer(),
          new SameTermFilterOptimizer(),
          new QueryModelNormalizer(),
          new IterativeEvaluationOptimizer(),
          new FilterOptimizer(),
          new OrderLimitOptimizer()
  );

  private final RDFTupleSerializer tupleSerializer;
  private final Map<Var,Term> varConversionCache = new HashMap<>();
  private int count = 0;

  public SPARQLConverter(RDFTupleSerializer tupleSerializer) {
    this.tupleSerializer = tupleSerializer;
  }

  public Program convert(String query, String resultRelation) {
    ConversionResult result = convert(query);
    Program program = result.getProgram();
    result.results.forEach(resultTuple ->
      program.addRule(new Rule(newTuple(resultRelation, resultTuple.vars), newTuple(resultTuple)))
    );
    return program;
  }

  private ConversionResult convert(String query) {
    QueryParser queryParser = new SPARQLParser();
    ParsedQuery parsedQuery = queryParser.parseQuery(query, null);
    TupleExpr tupleExpr = parsedQuery.getTupleExpr();
    Dataset dataset = parsedQuery.getDataset();
    BindingSet bindingSet = new EmptyBindingSet();
    OPTIMIZERS.forEach(optimizer -> optimizer.optimize(tupleExpr, dataset, bindingSet));
    return convert(parsedQuery.getTupleExpr());
  }

  private ConversionResult convert(TupleExpr expr) {
    if (expr instanceof ArbitraryLengthPath) {
      return convert((ArbitraryLengthPath) expr);
    } else if (expr instanceof BindingSetAssignment) {
      return convert((BindingSetAssignment) expr);
    } else if (expr instanceof Difference) {
      return convert((Difference) expr);
    } else if(expr instanceof EmptySet) {
      return convert((EmptySet) expr);
    } else if(expr instanceof Filter) {
      return convert((Filter) expr);
    } else if(expr instanceof Join) {
      return convert((Join) expr);
    } else if(expr instanceof LeftJoin) {
      return convert((LeftJoin) expr);
    } else if(expr instanceof MultiProjection) {
      return convert((MultiProjection) expr);
    } else if(expr instanceof Projection) {
      return convert((Projection) expr);
    } else if(expr instanceof StatementPattern) {
      return convert((StatementPattern) expr);
    } else if(expr instanceof Union) {
      return convert((Union) expr);
    } else {
      throw new UnsupportedOperationException("Not supported TupleExpr: " + expr);
    }
  }

  private ConversionResult convert(ArbitraryLengthPath arbitraryLengthPath) {
    throw new UnsupportedOperationException("ArbitraryLengthPath is not supported yet");
  }

  private ConversionResult convert(BindingSetAssignment bindingSetAssignment) {
    throw new UnsupportedOperationException("BindingSetAssignment is not supported yet");
  }

  private ConversionResult convert(Difference difference) {
    ConversionResult left = convert(difference.getLeftArg());
    ConversionResult right = convert(difference.getRightArg());
    Set<Rule> rules = merge(left.rules, right.rules);
    return new ConversionResult(left.results.stream().flatMap(resultLeft -> right.results.stream().map(resultRight -> {
      String filterTupleName = newTupleName();
      String rightFilterTupleName = newTupleName();
      List<Var> rightFilterVars = intersected(resultLeft.vars, resultRight.vars);
      rules.add(new Rule(newTuple(filterTupleName, resultLeft.vars), newTuple(resultLeft), newNegatedTuple(rightFilterTupleName, rightFilterVars)));
      rules.add(new Rule(newTuple(rightFilterTupleName, rightFilterVars), newTuple(resultRight)));
      return new ResultTuple(filterTupleName, resultLeft.vars);
    })).collect(Collectors.toSet()), rules);
  }

  private ConversionResult convert(EmptySet emptySet) {
    return new ConversionResult(Collections.emptySet(), Collections.emptySet());
  }

  private ConversionResult convert(Filter filter) {
    throw new UnsupportedOperationException("Filter is not supported yet");
  }

  private ConversionResult convert(Join join) {
    ConversionResult left = convert(join.getLeftArg());
    ConversionResult right = convert(join.getRightArg());
    Set<Rule> rules = merge(left.rules, right.rules);
    return new ConversionResult(left.results.stream().flatMap(resultLeft -> right.results.stream().map(resultRight -> {
      String tupleName = newTupleName();
      List<Var> resultVars = mergeWithDeduplication(resultLeft.vars, resultRight.vars);
      rules.add(new Rule(newTuple(tupleName, resultVars), newTuple(resultLeft), newTuple(resultRight)));
      return new ResultTuple(tupleName, resultVars);
    })).collect(Collectors.toSet()), rules);
  }

  private ConversionResult convert(LeftJoin leftJoin) {
    ConversionResult left = convert(leftJoin.getLeftArg());
    ConversionResult right = convert(leftJoin.getRightArg());
    Set<Rule> rules = merge(left.rules, right.rules);
    return new ConversionResult(left.results.stream().flatMap(resultLeft -> right.results.stream().flatMap(resultRight -> {
      String joinedTupleName = newTupleName();
      String leftTupleName = newTupleName();
      String rightFilterTupleName = newTupleName();
      List<Var> allResultVars = mergeWithDeduplication(resultLeft.vars, resultRight.vars);
      List<Var> rightFilterVars = intersected(resultLeft.vars, resultRight.vars);
      rules.add(new Rule(newTuple(joinedTupleName, allResultVars), newTuple(resultLeft), newTuple(resultRight)));
      rules.add(new Rule(newTuple(leftTupleName, resultLeft.vars), newTuple(resultLeft), newNegatedTuple(rightFilterTupleName, rightFilterVars)));
      rules.add(new Rule(newTuple(rightFilterTupleName, rightFilterVars), newTuple(resultRight)));
      return Stream.of(new ResultTuple(joinedTupleName, allResultVars), new ResultTuple(leftTupleName, resultLeft.vars));
    })).collect(Collectors.toSet()), rules);
  }

  private ConversionResult convert(MultiProjection multiProjection) {
    throw new UnsupportedOperationException("MultiProjection is not supported yet");
  }

  private ConversionResult convert(Projection projection) {
    ConversionResult arg = convert(projection.getArg());
    String tupleName = newTupleName();
    List<Var> resultVars = projection.getProjectionElemList().getElements().stream()
            .map(projectionElem -> new Var(projectionElem.getTargetName()))
            .collect(Collectors.toList());
    return new ConversionResult(new ResultTuple(tupleName, resultVars), merge(arg.rules,
            arg.results.stream().map(result -> new Rule(newTuple(tupleName, resultVars), newTuple(result))
    )));
  }

  private ConversionResult convert(StatementPattern pattern) {
    String tupleName = newTupleName();
    List<Var> resultVars = new ArrayList<>();
    if(!pattern.getSubjectVar().isConstant()) {
      resultVars.add(pattern.getSubjectVar());
    }
    if(!pattern.getPredicateVar().isConstant()) {
      resultVars.add(pattern.getPredicateVar());
    }
    if(!pattern.getObjectVar().isConstant()) {
      resultVars.add(pattern.getObjectVar());
    }
    //TODO: graph

    return new ConversionResult(new ResultTuple(tupleName, resultVars), new Rule(newTuple(tupleName, resultVars), tupleSerializer.convertTriple(
            convert(pattern.getSubjectVar()),
            convert(pattern.getPredicateVar()),
            convert(pattern.getObjectVar())
    )));
  }

  private ConversionResult convert(Union union) {
    ConversionResult left = convert(union.getLeftArg());
    ConversionResult right = convert(union.getRightArg());
    return new ConversionResult(merge(left.results,right.results), merge(left.rules, right.rules));
  }

  private Term convert(Var var) {
    return varConversionCache.computeIfAbsent(var, (variable) -> {
      if(var.isConstant()) {
        return convert(var.getValue());
      } else {
        return new Variable(var.getName());
      }
    });
  }

  private Term convert(Value value) {
    return tupleSerializer.convertTerm(RDF4J.asRDFTerm(value, SALT));
  }

  private CompoundTerm newTuple(ResultTuple tuple) {
    return newTuple(tuple.name, tuple.vars);
  }

  private CompoundTerm newTuple(String name, List<Var> varList) {
    return new CompoundTerm(name, varList.stream().map(this::convert).toArray(Term[]::new));
  }

  private CompoundTerm newNegatedTuple(String name, List<Var> varList) {
    return new CompoundTerm(name, false, varList.stream().map(this::convert).toArray(Term[]::new));
  }

  private String newTupleName() {
    return "tuple" + (count++);
  }

  private<T> List<T> mergeWithDeduplication(List<T> a, List<T> b) {
    List<T> result = new ArrayList<>(a);
    for(T e : b) {
      if(!result.contains(e)) {
        result.add(e);
      }
    }
    return result;
  }

  private<T> List<T> intersected(List<T> a, List<T> b) {
    List<T> result = new ArrayList<>();
    for(T e : a) {
      if(b.contains(e)) {
        result.add(e);
      }
    }
    return result;
  }

  private<T> Set<T> merge(Set<T> a, Set<T> b) {
    Set<T> result = new HashSet<>(a);
    result.addAll(b);
    return result;
  }

  private <T> Set<T> merge(Set<T> a, Stream<T> b) {
    Set<T> result = new HashSet<>(a);
    b.forEach(result::add);
    return result;
  }

  public static class ConversionResult {
    private Set<ResultTuple> results;
    private Set<Rule> rules;

    ConversionResult(Set<ResultTuple> results, Set<Rule> rules) {
      this.results = results;
      this.rules = rules;
    }

    ConversionResult(ResultTuple result, Set<Rule> rules) {
      this(Collections.singleton(result), rules);
    }

    ConversionResult(ResultTuple result, Rule rule) {
      this(Collections.singleton(result), Collections.singleton(rule));
    }

    public Program getProgram() {
      return new Program(rules.stream());
    }
  }

  public static class ResultTuple {
    private String name;
    private List<Var> vars;

    ResultTuple(String name, List<Var> vars) {
      this.name = name;
      this.vars = vars;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof ResultTuple && name.equals(((ResultTuple) obj).name) && vars.equals(((ResultTuple) obj).vars);
    }

    @Override
    public int hashCode() {
      return name.hashCode() ^ vars.hashCode();
    }
  }
}
