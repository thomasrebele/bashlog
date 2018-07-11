package rdf;

import com.google.common.collect.Sets;
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
  private static final Constant<Comparable<?>> NULL = new Constant<>("NULL");
  private static final Var NULL_VAR = new Var(SPARQLConverter.class.getCanonicalName() + "#null");
  private static final Var ALL_VALUES_VAR = new Var(SPARQLConverter.class.getCanonicalName() + "#any");
  private static final ConversionResult EMPTY_SET = new ConversionResult();

  private final RDFTupleSerializer tupleSerializer;
  private final Map<Var, Term> varConversionCache = new HashMap<>();
  private final String helperPredicatePrefix;
  private final ResultTuple allValuesResult;
  private int count = 0;

  public SPARQLConverter(RDFTupleSerializer tupleSerializer) {
    this(tupleSerializer, "tmp_predicate_");
  }

  public SPARQLConverter(RDFTupleSerializer tupleSerializer, String helperPredicatePrefix) {
    this.tupleSerializer = tupleSerializer;
    this.helperPredicatePrefix = helperPredicatePrefix;
    allValuesResult = new ResultTuple(helperPredicatePrefix + "all", Collections.singletonList(ALL_VALUES_VAR));
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
    return convert(tupleExpr);
  }

  private ConversionResult convert(TupleExpr expr) {
    if (expr instanceof ArbitraryLengthPath) {
      return convert((ArbitraryLengthPath) expr);
    } else if (expr instanceof BindingSetAssignment) {
      return convert((BindingSetAssignment) expr);
    } else if (expr instanceof Difference) {
      return convert((Difference) expr);
    } else if (expr instanceof Distinct) {
      return convert((Distinct) expr);
    } else if (expr instanceof EmptySet) {
      return EMPTY_SET;
    } else if (expr instanceof Extension) {
      return convert((Extension) expr);
    } else if (expr instanceof Filter) {
      return convert((Filter) expr);
    } else if (expr instanceof Join) {
      return convert((Join) expr);
    } else if (expr instanceof LeftJoin) {
      return convert((LeftJoin) expr);
    } else if (expr instanceof MultiProjection) {
      return convert((MultiProjection) expr);
    } else if (expr instanceof Projection) {
      return convert((Projection) expr);
    } else if (expr instanceof SingletonSet) {
      return singletonSet();
    } else if (expr instanceof StatementPattern) {
      return convert((StatementPattern) expr);
    } else if (expr instanceof Union) {
      return convert((Union) expr);
    } else if (expr instanceof ZeroLengthPath) {
      return convert((ZeroLengthPath) expr);
    } else {
      throw new UnsupportedOperationException("Not supported TupleExpr: " + expr);
    }
  }

  private ConversionResult convert(ArbitraryLengthPath arbitraryLengthPath) {
    TupleExpr pathExpression = arbitraryLengthPath.getPathExpression();
    Var subjectVar = arbitraryLengthPath.getSubjectVar();
    Var objectVar = arbitraryLengthPath.getObjectVar();

    // Replace constant with variables in the pathExpression
    if (subjectVar.hasValue()) {
      Var newVar = new Var(subjectVar.getName());
      pathExpression.replaceChildNode(subjectVar, newVar);
    }
    if (objectVar.hasValue()) {
      Var newVar = new Var(objectVar.getName());
      pathExpression.replaceChildNode(objectVar, newVar);
    }

    // Converts the inner expression
    ConversionResult innerPath = convert(pathExpression);
    Set<Rule> rules = new HashSet<>(innerPath.rules);
    String innerRelation = newTupleName();
    innerPath.results.forEach(result -> rules.add(new Rule(newTuple(innerRelation, result.vars), newTuple(result))));

    //Build the closure
    if (subjectVar.hasValue()) {
      String tupleName = newTupleName();

      //Init
      if (arbitraryLengthPath.getMinLength() == 0) {
        rules.add(new Rule(newTuple(tupleName, Collections.singletonList(subjectVar))));
      } else {
        List<CompoundTerm> body = new ArrayList<>();
        for (long i = 0; i < arbitraryLengthPath.getMinLength(); i++) {
          body.add(newTuple(innerRelation, Arrays.asList(i == 0 ? subjectVar : new Var("C" + i), new Var("C" + (i + 1)))));
        }
        rules.add(new Rule(newTuple(tupleName, Collections.singletonList(new Var("C" + arbitraryLengthPath.getMinLength()))), body));
      }

      // Recursion
      Var left = new Var("Left");
      Var right = new Var("Right");
      rules.add(new Rule(
              newTuple(tupleName, Collections.singletonList(right)),
              newTuple(tupleName, Collections.singletonList(left)),
              newTuple(innerRelation, Arrays.asList(left, right))
      ));

      //Build final
      if (objectVar.hasValue()) {
        String finalTupleName = newTupleName();
        rules.add(new Rule(newTuple(finalTupleName, Collections.emptyList()), newTuple(tupleName, Collections.singletonList(objectVar))));
        return new ConversionResult(new ResultTuple(finalTupleName, Collections.emptyList()), rules);
      } else {
        return new ConversionResult(new ResultTuple(tupleName, Collections.singletonList(objectVar)), rules);

      }
    } else if (objectVar.hasValue()) {
      String tupleName = newTupleName();

      //Init
      if (arbitraryLengthPath.getMinLength() == 0) {
        rules.add(new Rule(newTuple(tupleName, Collections.singletonList(objectVar))));
      } else {
        List<CompoundTerm> body = new ArrayList<>();
        for (long i = 0; i < arbitraryLengthPath.getMinLength(); i++) {
          body.add(newTuple(innerRelation, Arrays.asList(new Var("C" + (i + 1)), i == 0 ? subjectVar : new Var("C" + i))));
        }
        rules.add(new Rule(newTuple(tupleName, Collections.singletonList(new Var("C" + arbitraryLengthPath.getMinLength()))), body));
      }

      // Recursion
      Var left = new Var("Left");
      Var right = new Var("Right");
      rules.add(new Rule(
              newTuple(tupleName, Collections.singletonList(left)),
              newTuple(innerRelation, Arrays.asList(left, right)),
              newTuple(tupleName, Collections.singletonList(right))
      ));

      //Build final
      return new ConversionResult(new ResultTuple(tupleName, Collections.singletonList(subjectVar)), rules);
    } else {
      String tupleName = newTupleName();

      //Init
      if (arbitraryLengthPath.getMinLength() == 0) {
        rules.addAll(allValuesRules());
        rules.add(new Rule(newTuple(tupleName, Arrays.asList(ALL_VALUES_VAR, ALL_VALUES_VAR)), newTuple(allValuesResult)));
      } else {
        List<CompoundTerm> body = new ArrayList<>();
        for (long i = 0; i < arbitraryLengthPath.getMinLength(); i++) {
          body.add(newTuple(innerRelation, Arrays.asList(new Var("C" + i), new Var("C" + (i + 1)))));
        }
        rules.add(new Rule(newTuple(tupleName, Arrays.asList(new Var("C0"), new Var("C" + arbitraryLengthPath.getMinLength()))), body));
      }

      // Recursion
      Var left = new Var("Left");
      Var middle = new Var("Middle");
      Var right = new Var("Right");
      rules.add(new Rule(
              newTuple(tupleName, Arrays.asList(left, right)),
              newTuple(innerRelation, Arrays.asList(left, middle)),
              newTuple(tupleName, Arrays.asList(middle, right))
      ));

      //Build final
      return new ConversionResult(new ResultTuple(tupleName, Arrays.asList(subjectVar, objectVar)), rules);
    }
  }

  private ConversionResult convert(BindingSetAssignment bindingSetAssignment) {
    throw new UnsupportedOperationException("BindingSetAssignment is not supported yet: " + bindingSetAssignment);
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

  private ConversionResult convert(Distinct distinct) {
    return convert(distinct.getArg());
  }

  private ConversionResult convert(Extension extension) {
    ConversionResult arg = convert(extension.getArg());
    Set<Rule> rules = new HashSet<>(arg.rules);
    return new ConversionResult(extension.getElements().stream().flatMap(element -> {
      String bindTupleName = newTupleName();
      List<Var> bindVars = Collections.singletonList(new Var(element.getName()));
      ValueExpr val = element.getExpr();
      if (bindVars.equals(Collections.singletonList(val))) {
        //No op
        return arg.results.stream();
      } else if (val instanceof ValueConstant) {
        rules.add(new Rule(newTuple(bindTupleName, Collections.singletonList(new Var(bindTupleName, ((ValueConstant) val).getValue())))));
      } else {
        throw new UnsupportedOperationException("Not supported Extension: " + val);
      }
      return arg.results.stream().map(result -> {
        String outTupleName = newTupleName();
        List<Var> outVars = mergeWithDeduplication(result.vars, bindVars);
        rules.add(new Rule(newTuple(outTupleName, outVars), newTuple(result), newTuple(bindTupleName, bindVars)));
        return new ResultTuple(outTupleName, outVars);
      });
    }).collect(Collectors.toSet()), rules);
  }

  private ConversionResult convert(Filter filter) {
    throw new UnsupportedOperationException("Filter is not supported yet: " + filter);
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
    throw new UnsupportedOperationException("MultiProjection is not supported yet: " + multiProjection);
  }

  private ConversionResult convert(Projection projection) {
    ConversionResult arg = convert(projection.getArg());
    String tupleName = newTupleName();
    List<Var> resultVars = projection.getProjectionElemList().getElements().stream()
            .map(projectionElem -> new Var(projectionElem.getTargetName()))
            .collect(Collectors.toList());
    return new ConversionResult(new ResultTuple(tupleName, resultVars), merge(arg.rules,
            arg.results.stream().map(result -> new Rule(newTuple(tupleName, resultVars.stream().map(var ->
                            result.vars.contains(var) ? var : NULL_VAR
                    ).collect(Collectors.toList())), newTuple(result))
            )));
  }

  private ConversionResult convert(StatementPattern pattern) {
    String tupleName = newTupleName();
    List<Var> resultVars = new ArrayList<>();
    if (!pattern.getSubjectVar().hasValue()) {
      resultVars.add(pattern.getSubjectVar());
    }
    if (!pattern.getPredicateVar().hasValue()) {
      resultVars.add(pattern.getPredicateVar());
    }
    if (!pattern.getObjectVar().hasValue()) {
      resultVars.add(pattern.getObjectVar());
    }
    //TODO: context

    return new ConversionResult(new ResultTuple(tupleName, resultVars), new Rule(newTuple(tupleName, resultVars), tupleSerializer.convertTriple(
            convert(pattern.getSubjectVar()),
            convert(pattern.getPredicateVar()),
            convert(pattern.getObjectVar())
    )));
  }

  private ConversionResult singletonSet() {
    String tupleName = newTupleName();
    return new ConversionResult(new ResultTuple(tupleName, Collections.emptyList()), new Rule(newTuple(tupleName, Collections.emptyList())));
  }

  private ConversionResult convert(Union union) {
    ConversionResult left = convert(union.getLeftArg());
    ConversionResult right = convert(union.getRightArg());
    return new ConversionResult(merge(left.results, right.results), merge(left.rules, right.rules));
  }

  private ConversionResult convert(ZeroLengthPath zeroLengthPath) {
    String tupleName = newTupleName();
    if (zeroLengthPath.getSubjectVar().hasValue()) {
      if (zeroLengthPath.getObjectVar().hasValue()) {
        if (zeroLengthPath.getSubjectVar().getValue().equals(zeroLengthPath.getObjectVar().getValue())) {
          return new ConversionResult(new ResultTuple(tupleName, Collections.emptyList()), new Rule(newTuple(tupleName, Collections.emptyList())));
        } else {
          return new ConversionResult();
        }
      } else {
        return new ConversionResult(new ResultTuple(tupleName, Collections.singletonList(zeroLengthPath.getObjectVar())), new Rule(newTuple(tupleName, Collections.singletonList(zeroLengthPath.getSubjectVar()))));
      }
    } else {
      if (zeroLengthPath.getObjectVar().hasValue()) {
        return new ConversionResult(new ResultTuple(tupleName, Collections.singletonList(zeroLengthPath.getSubjectVar())), new Rule(newTuple(tupleName, Collections.singletonList(zeroLengthPath.getObjectVar()))));
      } else {
        return new ConversionResult(new ResultTuple(tupleName, Arrays.asList(zeroLengthPath.getSubjectVar(), zeroLengthPath.getObjectVar())), merge(allValuesRules(), new Rule(newTuple(tupleName, Arrays.asList(ALL_VALUES_VAR, ALL_VALUES_VAR)), newTuple(allValuesResult))));
      }
    }
  }

  private Term convert(Var var) {
    return varConversionCache.computeIfAbsent(var, (variable) -> {
      if (var == NULL_VAR) {
        return NULL;
      } else if (var.hasValue()) {
        return convert(var.getValue());
      } else {
        return new Variable(var.getName());
      }
    });
  }

  private Term convert(Value value) {
    return tupleSerializer.convertTerm(RDF4J.asRDFTerm(value, SALT));
  }

  private Set<Rule> allValuesRules() {
    Variable allValues = new Variable(ALL_VALUES_VAR.getName());
    Variable anySubject = new Variable("AnySubj");
    Variable anyPred = new Variable("AnyPred");
    Variable anyObj = new Variable("AnyObj");
    return Sets.newHashSet(
            new Rule(newTuple(allValuesResult), tupleSerializer.convertTriple(allValues, anyPred, anyObj)),
            new Rule(newTuple(allValuesResult), tupleSerializer.convertTriple(anySubject, allValues, anyObj)),
            new Rule(newTuple(allValuesResult), tupleSerializer.convertTriple(anySubject, anyPred, allValues))
    );
  }

  private CompoundTerm newTuple(ResultTuple tuple) {
    return newTuple(tuple.name, tuple.vars);
  }

  private CompoundTerm newTuple(String name, List<Var> varList) {
    return new CompoundTerm(name, varList.stream().map(this::convert).toArray(Term[]::new));
  }

  private CompoundTerm newNegatedTuple(String name, List<Var> varList) {
    return new CompoundTerm(name, true, varList.stream().map(this::convert).toArray(Term[]::new));
  }

  private String newTupleName() {
    return helperPredicatePrefix + (count++);
  }

  private <T> List<T> mergeWithDeduplication(List<T> a, List<T> b) {
    List<T> result = new ArrayList<>(a);
    for (T e : b) {
      if (!result.contains(e)) {
        result.add(e);
      }
    }
    return result;
  }

  private <T> List<T> intersected(List<T> a, List<T> b) {
    List<T> result = new ArrayList<>();
    for (T e : a) {
      if (b.contains(e)) {
        result.add(e);
      }
    }
    return result;
  }

  private <T> Set<T> merge(Set<T> a, Set<T> b) {
    Set<T> result = new HashSet<>(a);
    result.addAll(b);
    return result;
  }

  private <T> Set<T> merge(Set<T> a, Stream<T> b) {
    Set<T> result = new HashSet<>(a);
    b.forEach(result::add);
    return result;
  }

  private <T> Set<T> merge(Set<T> a, T b) {
    Set<T> result = new HashSet<>(a);
    result.add(b);
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

    ConversionResult() {
      this(Collections.emptySet(), Collections.emptySet());
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
