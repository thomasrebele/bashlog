package rdf;

import common.parser.*;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.semanticweb.owlapi.io.RDFLiteral;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.semanticweb.owlapi.vocab.OWLRDFVocabulary.*;

/**
 * From https://www.w3.org/TR/owl2-profiles/#OWL_2_RL
 * TODO/: finish Table 6
 */
public class OntologyConverter {

  private static final boolean WITH_SAME_AS = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(OntologyConverter.class);
  private static final CompoundTerm FALSE = new CompoundTerm("false");


  private OWLOntology ontology;
  private final RDFTupleSerializer tupleSerializer;
  private int seed = 0;

  public OntologyConverter(RDFTupleSerializer tupleSerializer) {
    this.tupleSerializer = tupleSerializer;
  }

  private Stream<OWLProperty> allProperties() {
    //TODO: annotation properties?
    return Stream.concat(
            ontology.objectPropertiesInSignature(),
            ontology.dataPropertiesInSignature()
    );
  }

  private CompoundTerm term(Term subject, OWLRDFVocabulary property, Term object) {
    return tupleSerializer.convertTriple(subject, property.getIRI(), object);
  }

  private CompoundTerm term(Term subject, OWLPropertyExpression property, Term object) {
    if (property instanceof HasIRI) {
      return tupleSerializer.convertTriple(subject, ((HasIRI) property).getIRI(), object);
    } else if (property instanceof OWLObjectInverseOf) {
      return term(object, ((OWLObjectInverseOf) property).getInverse(), subject);
    } else {
      throw new IllegalArgumentException("Not able to build term for property expression: " + property);
    }
  }

  private CompoundTerm term(Term subject, OWLPropertyExpression property, OWLIndividual object) {
    return term(subject, property, tupleSerializer.convertTerm(convert(object)));
  }

  private CompoundTerm term(Term subject, OWLPropertyExpression property, OWLLiteral object) {
    if (property instanceof HasIRI) {
      return tupleSerializer.convertTriple(subject, ((HasIRI) property).getIRI(), new RDFLiteral(object));
    } else {
      throw new IllegalArgumentException("Not able to build term for property expression: " + property);
    }
  }

  private CompoundTerm term(HasIRI subject, OWLRDFVocabulary property, HasIRI object) {
    return tupleSerializer.convertTriple(subject.getIRI(), property.getIRI(), object.getIRI());
  }

  private CompoundTerm term(Term subject, OWLRDFVocabulary property, HasIRI object) {
    return tupleSerializer.convertTriple(subject, property.getIRI(), object.getIRI());
  }

  private CompoundTerm bind(OWLIndividual object, Term as) {
    return new BindTerm(tupleSerializer.convertTerm(convert(object)), as);
  }

  private BlankNodeOrIRI convert(OWLIndividual individual) {
    if(individual instanceof OWLNamedIndividual) {
      return ((OWLNamedIndividual) individual).getIRI();
    } else if(individual instanceof OWLAnonymousIndividual) {
      return new BlankNode() {
        @Override
        public String uniqueReference() {
          return ((OWLAnonymousIndividual) individual).getID().getID();
        }

        @Override
        public String ntriplesString() {
          return "_:" + uniqueReference();
        }
      };
    } else {
      throw new IllegalArgumentException(individual + " is not a valid individual");
    }
  }

  public Program convert(OWLOntology ontology) {
    this.ontology = ontology;

    Program program = new Program();

    // We add OWL Rules
    Variable s = new Variable("S");
    Variable s1 = new Variable("S1");
    Variable s2 = new Variable("S2");
    Variable o = new Variable("O");
    Variable o1 = new Variable("O1");
    Variable o2 = new Variable("O2");
    Variable x = new Variable("X");
    Variable y = new Variable("Y");
    Variable z = new Variable("Z");


    // Table 4. The Semantics of Equality
    if (WITH_SAME_AS) {
      // eq-ref
      allProperties().forEach(pr -> {
        program.addRule(new Rule(term(s, OWL_SAME_AS, s), term(s, pr, o)));
        program.addRule(new Rule(term(pr, OWL_SAME_AS, pr)));
      });
      ontology.objectPropertiesInSignature().forEach(pr ->
              program.addRule(new Rule(term(o, OWL_SAME_AS, o), term(s, pr, o))));

      // eq-sym
      program.addRule(new Rule(term(x, OWL_SAME_AS, y), term(y, OWL_SAME_AS, x)));

      // eq-trans
      program.addRule(new Rule(term(x, OWL_SAME_AS, z), term(x, OWL_SAME_AS, y), term(y, OWL_SAME_AS, z)));

      // eq-rep-s
      allProperties().forEach(pr ->
              program.addRule(new Rule(term(s2, pr, o), term(s, OWL_SAME_AS, s2), term(s, pr, o))));

      // eq-rep-p
      allProperties().forEach(pr ->
              allProperties().forEach(pr2 -> {
                if (!pr.equals(pr2)) {
                  program.addRule(new Rule(term(s, pr2, o), term(pr, OWL_SAME_AS, pr2), term(s, pr, o)));
                }
              })
      );

      // eq-rep-o
      ontology.objectPropertiesInSignature().forEach(pr ->
              program.addRule(new Rule(term(s, pr, o2), term(o, OWL_SAME_AS, o2), term(s, pr, o))));

      // eq-diff1
      program.addRule(new Rule(FALSE, term(x, OWL_SAME_AS, y), term(x, OWL_DIFFERENT_FROM, y)));

      //TODO: eq-diff2 and eq-diff3
    }


    // Table 5. The Semantics of Axioms about Properties
    //TODO: prp-ap

    // prp-dom
    Stream.concat(
            ontology.axioms(AxiomType.OBJECT_PROPERTY_DOMAIN),
            ontology.axioms(AxiomType.DATA_PROPERTY_DOMAIN)
    ).forEach(ax ->
            headRules(ax.getDomain(), s).forEach(headRule -> program.addRule(headRule.withBodyClause(term(s, ax.getProperty(), o))))
    );

    // prp-rng
    ontology.axioms(AxiomType.OBJECT_PROPERTY_RANGE).forEach(ax ->
            headRules(ax.getRange(), o).forEach(headRule -> program.addRule(headRule.withBodyClause(term(s, ax.getProperty(), o))))
    );
    //TODO: range of data properties???

    // prp-fp
    ontology.axioms(AxiomType.FUNCTIONAL_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(term(o1, OWL_SAME_AS, o2), term(s, ax.getProperty(), o1), term(s, ax.getProperty(), o2)))
    );
    //TODO: functional datatype properties

    // prp-ifp
    ontology.axioms(AxiomType.INVERSE_FUNCTIONAL_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(term(s1, OWL_SAME_AS, s2), term(s1, ax.getProperty(), o), term(s2, ax.getProperty(), o)))
    );

    // prp-irp
    ontology.axioms(AxiomType.IRREFLEXIVE_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(FALSE, term(x, ax.getProperty(), x)))
    );

    // prp-symp
    ontology.axioms(AxiomType.SYMMETRIC_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(term(o, ax.getProperty(), s), term(s, ax.getProperty(), o)))
    );

    // prp-asyp
    ontology.axioms(AxiomType.ASYMMETRIC_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(FALSE, term(x, ax.getProperty(), y), term(y, ax.getProperty(), x)))
    );

    // prp-trp
    ontology.axioms(AxiomType.TRANSITIVE_OBJECT_PROPERTY).forEach(ax ->
            program.addRule(new Rule(term(x, ax.getProperty(), z), term(x, ax.getProperty(), y), term(y, ax.getProperty(), z)))
    );

    // prp-spo1
    Stream.concat(
            ontology.axioms(AxiomType.SUB_DATA_PROPERTY),
            ontology.axioms(AxiomType.SUB_OBJECT_PROPERTY)
            //TODO: annotation properties
    ).forEach(ax ->
            program.addRule(new Rule(term(s, ax.getSuperProperty(), o), term(s, ax.getSubProperty(), o)))
    );

    //TODO: prp-spo2

    // prp-eqp1 and prp-eqp1
    Stream.concat(
            ontology.axioms(AxiomType.EQUIVALENT_DATA_PROPERTIES),
            ontology.axioms(AxiomType.EQUIVALENT_OBJECT_PROPERTIES)
    ).forEach(ax ->
            ax.properties().forEach(pr1 ->
                    ax.properties().forEach(pr2 -> {
                      if (!pr1.equals(pr2)) {
                        program.addRule(new Rule(term(s, pr1, o), term(s, pr2, o)));
                        program.addRule(new Rule(term(s, pr2, o), term(s, pr1, o)));
                      }
                    })
            )
    );

    // prp-pdw
    Stream.concat(
            ontology.axioms(AxiomType.DISJOINT_DATA_PROPERTIES),
            ontology.axioms(AxiomType.DISJOINT_OBJECT_PROPERTIES)
    ).forEach(ax ->
            ax.properties().forEach(pr1 ->
                    ax.properties().forEach(pr2 -> {
                      if (!pr1.equals(pr2)) {
                        program.addRule(new Rule(FALSE, term(s, pr1, o), term(s, pr2, o)));
                      }
                    })
            )
    );

    //TODO: prp-adp

    // prp-inv1 and prp-inv2
    ontology.axioms(AxiomType.INVERSE_OBJECT_PROPERTIES).forEach(ax -> {
      OWLPropertyExpression pr1 = ax.getFirstProperty();
      OWLPropertyExpression pr2 = ax.getSecondProperty();
      program.addRule(new Rule(term(o, pr2, s), term(s, pr1, o)));
      program.addRule(new Rule(term(o, pr1, s), term(s, pr2, o)));
    });

    // TODO: prp-key, prp-npa1, prp-npa2


    // Table 6. The Semantics of Classes

    // cls-nothing2
    // program.addRule(new Rule(FALSE, term(x, RDF_TYPE, OWL_NOTHING)));

    //TODO: cls-int1, cls-int2, cls-uni
    //TODO: other axioms


    // Table 7. The Semantics of Class Axioms
    // cax-sco
    ontology.axioms(AxiomType.SUBCLASS_OF).forEach(ax ->
      headRules(ax.getSuperClass(), x).forEach(headRule ->
              bodyTerm(ax.getSubClass(), x).forEach(body ->
                      program.addRule(headRule.withBodyClauses(body))
              )
      )
    );

    // cax-eqc1 and cax-eqc2
    ontology.axioms(AxiomType.EQUIVALENT_CLASSES).forEach(ax ->
      ax.classExpressions().forEach(cl1 ->
              ax.classExpressions().forEach(cl2 -> {
                if(!cl1.equals(cl2)) {
                  bodyTerm(cl1, x).forEach(body -> headRules(cl2, x).forEach(headRule -> program.addRule(headRule.withBodyClauses(body))));
                }
              })
      )
    );

    // cax-dw
    ontology.axioms(AxiomType.DISJOINT_CLASSES).forEach(ax ->
            ax.classExpressions().forEach(cl1 ->
                    ax.classExpressions().forEach(cl2 -> {
                      if(!cl1.equals(cl2)) {
                        bodyTerm(cl1, x).forEach(body1 -> bodyTerm(cl2, x).forEach(body2 -> program.addRule(new Rule(FALSE, union(body1, body2)))));
                      }
                    })
            )
    );

    //TODO: cat-adc

    //  Table 8. The Semantics of Datatypes
    // TODO

    return normalize(program);
  }

  private Stream<Rule> headRules(OWLClassExpression type, Term instance) {
    if(type instanceof OWLClass) {
      return Stream.of(new Rule(term(instance, RDF_TYPE, (OWLClass) type)));
    } else if(type instanceof OWLObjectIntersectionOf) {
      return ((OWLObjectIntersectionOf) type).operands().flatMap(cls -> headRules(cls, instance));
    } else if(type instanceof OWLObjectHasValue) {
      return Stream.of(new Rule(term(instance, ((OWLObjectHasValue) type).getProperty(), ((OWLObjectHasValue) type).getFiller())));
    } else if(type instanceof OWLDataHasValue) {
      return Stream.of(new Rule(term(instance, ((OWLDataHasValue) type).getProperty(), ((OWLDataHasValue) type).getFiller())));
    } else if(type instanceof OWLObjectMaxCardinality) {
      Variable filler = newVariable("filler");
      OWLPropertyExpression prop = ((OWLObjectMaxCardinality) type).getProperty();
      switch (((OWLObjectMaxCardinality) type).getCardinality()) {
        case 0:
          return bodyTerm(((OWLObjectMaxCardinality) type).getFiller(), filler).map(fillerClauses -> new Rule(FALSE, union(
                  Collections.singletonList(term(instance, prop, filler)),
                  fillerClauses
          )));
        case 1:
          return bodyTerm(((OWLObjectMaxCardinality) type).getFiller(), filler).map(fillerClauses -> new Rule(term(instance, OWL_SAME_AS, filler), union(
                  Collections.singletonList(not(term(instance, prop, filler))),
                  fillerClauses
          )));
        default:
          LOGGER.warn(((OWLObjectMaxCardinality) type).getCardinality() + " is not a supported cardinality in head expression: " + type);
          return Stream.empty();
      }
    } else if(type instanceof OWLObjectComplementOf) {
      return bodyTerm(((OWLObjectComplementOf) type).getOperand(), instance).map(filterClauses -> new Rule(FALSE, filterClauses));
    } else if(type instanceof OWLObjectAllValuesFrom) {
      Variable filler = newVariable("filler");
      OWLPropertyExpression prop = ((OWLObjectAllValuesFrom) type).getProperty();
      return headRules(((OWLObjectAllValuesFrom) type).getFiller(), filler).map(headRule -> headRule.withBodyClause(term(instance, prop, filler)));
    } else {
      LOGGER.warn("Not supported class expression in head: " + type);
      return Stream.empty();
    }
  }

  private Variable newVariable(String namePrefix) {
    seed++;
    return new Variable(namePrefix + seed);
  }

  private Stream<List<CompoundTerm>> bodyTerm(OWLClassExpression type, Term instance) {
    if(type instanceof OWLClass) {
      return Stream.of(Collections.singletonList(term(instance, RDF_TYPE, (OWLClass) type)));
    } else if(type instanceof OWLObjectIntersectionOf) {
      return ((OWLObjectIntersectionOf) type).operands().map(cls -> bodyTerm(cls, instance)).reduce((intersections1, intersections2) -> {
        List<List<CompoundTerm>> intersections2List = intersections2.collect(Collectors.toList());
        return intersections1.flatMap(inter1 -> intersections2List.stream().map(inter2 -> union(inter1, inter2)));
      }).orElseGet(Stream::empty);
    } else if(type instanceof OWLObjectUnionOf) {
      return ((OWLObjectUnionOf) type).operands().flatMap(cls -> bodyTerm(cls, instance));
    } else if(type instanceof OWLObjectSomeValuesFrom) {
      Variable var = newVariable("SomeValue");
      OWLPropertyExpression prop = ((OWLObjectSomeValuesFrom) type).getProperty();
      OWLClassExpression filler = ((OWLObjectSomeValuesFrom) type).getFiller();
      return bodyTerm(filler, var).map(fillerClause -> union(Collections.singletonList(term(instance, prop, var)), fillerClause));
    } else if(type instanceof OWLDataSomeValuesFrom) {
      Variable var = newVariable("SomeValue");
      OWLPropertyExpression prop = ((OWLDataSomeValuesFrom) type).getProperty();
      OWLDataRange filler = ((OWLDataSomeValuesFrom) type).getFiller();
      return bodyTerm(filler, var).map(fillerClause -> union(Collections.singletonList(term(instance, prop, var)), fillerClause));
    } else if(type instanceof OWLObjectHasValue) {
      OWLPropertyExpression prop = ((OWLObjectHasValue) type).getProperty();
      OWLIndividual filler = ((OWLObjectHasValue) type).getFiller();
      return Stream.of(Collections.singletonList(term(instance, prop, filler)));
    } else if(type instanceof OWLDataHasValue) {
      OWLPropertyExpression prop = ((OWLDataHasValue) type).getProperty();
      OWLLiteral filler = ((OWLDataHasValue) type).getFiller();
      return Stream.of(Collections.singletonList(term(instance, prop, filler)));
    } else if(type instanceof OWLObjectOneOf) {
      return ((OWLObjectOneOf) type).individuals().map(v -> Collections.singletonList(bind(v, instance)));
    } else {
      LOGGER.warn("Not supported class expression in body: " + type);
      return Stream.empty();
    }
  }

  private Stream<List<CompoundTerm>> bodyTerm(OWLDataRange type, Term instance) {
    //TODO: implement
    LOGGER.warn("Not supported data range: " + type);
    return Stream.empty();
  }

  private <T> List<T> union(List<T> a, List<T> b) {
    List<T> result = new ArrayList<>(a);
    result.addAll(b);
    return result;
  }

  private CompoundTerm not(CompoundTerm term) {
    return new CompoundTerm(term.name, !term.negated, term.args);
  }

  /**
   * Replaces replacement terms
   */
  private Program normalize(Program program) {
    return new Program(program.rules().stream().map(rule -> {
      Map<Term,Term> replacement = new HashMap<>();
      rule.body.stream().filter(term -> term instanceof BindTerm).forEach(term -> {
        BindTerm bind = (BindTerm) term;
        if (replacement.containsKey(bind.getAs()) && !replacement.get(bind.getAs()).equals(bind.getTerm())) {
          LOGGER.warn("Invalid rule: " + rule);
        }
        replacement.put(bind.getAs(), bind.getTerm());
      });

      return new Rule(replace(rule.head, replacement), rule.body.stream().flatMap(term -> {
        if(term instanceof BindTerm) {
          return Stream.empty();
        } else {
          return Stream.of(replace(term, replacement));
        }
      }).collect(Collectors.toList()));
    }));
  }

  private CompoundTerm replace(CompoundTerm compoundTerm, Map<Term,Term> replacement) {
    return new CompoundTerm(compoundTerm.name, compoundTerm.negated, Arrays.stream(compoundTerm.args).map(term ->
            replacement.getOrDefault(term, term)).toArray(Term[]::new)
    );
  }

  private static class BindTerm extends CompoundTerm {
    BindTerm(Term term, Term as) {
      super("bind", term, as);
    }

    Term getTerm() {
      return args[0];
    }

    Term getAs() {
      return args[1];
    }
  }
}
