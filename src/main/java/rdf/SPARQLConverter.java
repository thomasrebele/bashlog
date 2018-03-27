package rdf;

import common.parser.*;
import org.apache.commons.rdf.rdf4j.RDF4J;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import java.util.*;

public class SPARQLConverter {

  private static final UUID SALT = UUID.randomUUID();
  private static final Constant<Comparable> NULL = new Constant<>(null);

  private final RDFTupleSerializer tupleSerializer;
  private final Map<Var,Term> varConversionCache = new HashMap<>();
  private int count = 0;

  public SPARQLConverter(RDFTupleSerializer tupleSerializer) {
    this.tupleSerializer = tupleSerializer;
  }

  public Program convert(String query, String resultRelation) {
    ConversionResult result = convert(query);
    Program program = result.getProgram();
    program.addRule(new Rule(newTuple(resultRelation, result.resultVars), newTuple(result.resultRelation, result.resultVars)));
    return program;
  }

  private ConversionResult convert(String query) {
    QueryParser queryParser = new SPARQLParser();
    ParsedQuery parsedQuery = queryParser.parseQuery(query, null);
    return convert(parsedQuery.getTupleExpr());
  }

  private ConversionResult convert(TupleExpr expr) {
    if(expr instanceof Join) {
      return convert((Join) expr);
    } else if(expr instanceof Projection) {
      return convert((Projection) expr);
    } else if(expr instanceof StatementPattern) {
      return convert((StatementPattern) expr);
    } else {
      throw new UnsupportedOperationException("Not supported TupleExpr: " + expr);
    }
  }

  private ConversionResult convert(Join join) {
    ConversionResult left = convert(join.getLeftArg());
    ConversionResult right = convert(join.getRightArg());
    String tupleName = newTupleName();
    List<Var> resultVars = mergeWithDeduplication(left.resultVars, right.resultVars);
    return new ConversionResult(tupleName, resultVars, merge(
            left.rules, right.rules,
            new Rule(newTuple(tupleName, resultVars), newTuple(left.resultRelation, left.resultVars), newTuple(right.resultRelation, right.resultVars))
    ));
  }

  private ConversionResult convert(Projection projection) {
    ConversionResult arg = convert(projection.getArg());
    String tupleName = newTupleName();
    List<Var> resultVars = new ArrayList<>();
    projection.getProjectionElemList().getElements().forEach(projectionElem -> resultVars.add(new Var(projectionElem.getTargetName())));
    if(resultVars.equals(arg.resultVars)) {
      return arg; //Identity projection
    }
    return new ConversionResult(tupleName, resultVars, merge(arg.rules,
            new Rule(newTuple(tupleName, resultVars), newTuple(arg.resultRelation, arg.resultVars))
    ));
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

    return new ConversionResult(tupleName, resultVars, new Rule(newTuple(tupleName, resultVars), tupleSerializer.convertTriple(
            convert(pattern.getSubjectVar()),
            convert(pattern.getPredicateVar()),
            convert(pattern.getObjectVar())
    )));
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

  private CompoundTerm newTuple(String name, List<Var> varList) {
    return new CompoundTerm(name, varList.stream().map(this::convert).toArray(Term[]::new));
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

  private<T> Set<T> merge(Set<T> a, T element) {
    Set<T> result = new HashSet<>(a);
    result.add(element);
    return result;
  }

  private<T> Set<T> merge(Set<T> a, Set<T> b, T element) {
    Set<T> result = new HashSet<>(a);
    result.addAll(b);
    result.add(element);
    return result;
  }

  public static class ConversionResult {
    private String resultRelation;
    private List<Var> resultVars;
    private Set<Rule> rules;

    ConversionResult(String resultRelation, List<Var> resultVars, Set<Rule> rules) {
      this.resultRelation = resultRelation;
      this.resultVars = resultVars;
      this.rules = rules;
    }

    ConversionResult(String resultRelation, List<Var> resultVars, Rule rule) {
      this(resultRelation, resultVars, Collections.singleton(rule));
    }

    public Program getProgram() {
      return new Program(rules.stream());
    }
  }
}
