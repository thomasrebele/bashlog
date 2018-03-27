package rdf;

import common.parser.*;
import org.semanticweb.owlapi.io.RDFResource;
import org.semanticweb.owlapi.io.RDFResourceIRI;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.rdf.RDFRendererBase;

import java.util.HashSet;
import java.util.Set;

import static org.semanticweb.owlapi.vocab.OWLRDFVocabulary.*;

/**
 * From https://www.w3.org/TR/owl2-profiles/#OWL_2_RL
 * TODO/: finish Table 6
 */
public class OWL2RLOntologyConverter {

  private static final CompoundTerm FALSE = new CompoundTerm("false");
  private static final HasIRI LOG_HAS_NODE = new RDFResourceIRI(org.semanticweb.owlapi.model.IRI.create("bashlog:hasNode"));
  private static final HasIRI LOG_HAS_ELEMENT = new RDFResourceIRI(org.semanticweb.owlapi.model.IRI.create("bashlog:hasElement"));

  private final RDFTupleSerializer tupleSerializer;

  public OWL2RLOntologyConverter(RDFTupleSerializer tupleSerializer) {
    this.tupleSerializer = tupleSerializer;
  }

  private CompoundTerm term(Term subject, Term property, Term object) {
    return tupleSerializer.convertTriple(subject, property, object);
  }

  private CompoundTerm term(Term subject, HasIRI property, Term object) {
    return tupleSerializer.convertTriple(subject, property.getIRI(), object);
  }

  private CompoundTerm term(Term subject, HasIRI property, HasIRI object) {
    return tupleSerializer.convertTriple(subject, property.getIRI(), object.getIRI());
  }

  private CompoundTerm term(HasIRI subject, HasIRI property, Term object) {
    return tupleSerializer.convertTriple(subject.getIRI(), property.getIRI(), object);
  }

  private CompoundTerm term(HasIRI subject, HasIRI property, HasIRI object) {
    return tupleSerializer.convertTriple(subject.getIRI(), property.getIRI(), object.getIRI());
  }

  public Program convert(OWLOntology ontology) {
    Variable c = new Variable("C");
    Variable c1 = new Variable("C1");
    Variable c2 = new Variable("C2");
    Variable c3 = new Variable("C3");
    Variable ci = new Variable("CI");
    Variable i1 = new Variable("I1");
    Variable i2 = new Variable("I2");
    Variable s = new Variable("S");
    Variable s1 = new Variable("S1");
    Variable s2 = new Variable("S2");
    Variable o = new Variable("O");
    Variable o1 = new Variable("O1");
    Variable o2 = new Variable("O2");
    Variable p = new Variable("P");
    Variable p1 = new Variable("P1");
    Variable p2 = new Variable("P2");
    Variable p3 = new Variable("P3");
    Variable u = new Variable("U");
    Variable v = new Variable("V");
    Variable x = new Variable("X");
    Variable y = new Variable("Y");
    Variable z = new Variable("Z");
    Variable l = new Variable("L");
    Variable e = new Variable("E");
    Variable n = new Variable("N");
    Variable n1 = new Variable("N1");
    Variable n2 = new Variable("N2");


    // We serialize the ontology
    ProgramRenderer programRenderer = new ProgramRenderer(ontology);
    programRenderer.render();
    Program program = programRenderer.getProgram();

    //We add utilities for list
    program.addRule(new Rule(term(l, LOG_HAS_NODE, l), term(l, RDF_REST, n)));
    program.addRule(new Rule(term(l, LOG_HAS_NODE, n2), term(l, LOG_HAS_NODE, n1), term(n1, RDF_REST, n2)));
    program.addRule(new Rule(term(l, LOG_HAS_ELEMENT, e), term(l, LOG_HAS_NODE, n), term(n, RDF_FIRST, e)));

    // We add OWL Rules

    // Table 4. The Semantics of Equality
    // eq-ref
    program.addRule(new Rule(term(s, OWL_SAME_AS, s), term(s, p, o)));
    program.addRule(new Rule(term(p, OWL_SAME_AS, p), term(s, p, o)));
    program.addRule(new Rule(term(o, OWL_SAME_AS, o), term(s, p, o)));

    // eq-sym
    program.addRule(new Rule(term(x, OWL_SAME_AS, y), term(y, OWL_SAME_AS, x)));

    // eq-trans
    program.addRule(new Rule(term(x, OWL_SAME_AS, z), term(x, OWL_SAME_AS, y), term(y, OWL_SAME_AS, z)));

    // eq-rep-s
    program.addRule(new Rule(term(s2, p, o), term(s, OWL_SAME_AS, s2), term(s, p, o)));

    // eq-rep-p
    program.addRule(new Rule(term(s, p2, o), term(p, OWL_SAME_AS, p2), term(s, p, o)));

    // eq-rep-o
    program.addRule(new Rule(term(s, p, o2), term(o, OWL_SAME_AS, o2), term(s, p, o)));

    // eq-dff1
    program.addRule(new Rule(FALSE, term(x, OWL_SAME_AS, y), term(x, OWL_DIFFERENT_FROM, y)));

    //TODO: eq-diff2 and eq-diff3


    // Table 5. The Semantics of Axioms about Properties
    //TODO: prp-ap

    // prp-dom
    program.addRule(new Rule(term(x, RDF_TYPE, c), term(p, RDFS_DOMAIN, c), term(x, p, y)));

    // prp-rng
    program.addRule(new Rule(term(y, RDF_TYPE, c), term(p, RDFS_RANGE, c), term(x, p, y)));

    // prp-fp
    program.addRule(new Rule(term(o1, OWL_SAME_AS, o2), term(p, RDF_TYPE, OWL_FUNCTIONAL_PROPERTY), term(s, p, o1), term(s, p, o2)));

    // prp-ifp
    program.addRule(new Rule(term(s1, OWL_SAME_AS, s2), term(p, RDF_TYPE, OWL_INVERSE_FUNCTIONAL_PROPERTY), term(s1, p, o), term(s2, p, o)));

    // prp-irp
    program.addRule(new Rule(FALSE, term(p, RDF_TYPE, OWL_IRREFLEXIVE_PROPERTY), term(x, p, x)));

    // prp-symp
    program.addRule(new Rule(term(s, p, o), term(p, RDF_TYPE, OWL_SYMMETRIC_PROPERTY), term(o, p, s)));

    // prp-asyp
    program.addRule(new Rule(FALSE, term(p, RDF_TYPE, OWL_ASYMMETRIC_PROPERTY), term(x, p, y), term(y, p, x)));

    // prp-trp
    program.addRule(new Rule(term(x, p, z), term(p, RDF_TYPE, OWL_TRANSITIVE_PROPERTY), term(x, p, y), term(y, p, z)));

    // prp-spo1
    program.addRule(new Rule(term(x, p2, y), term(p1, RDFS_SUB_PROPERTY_OF, p2), term(x, p1, y)));

    //TODO: prp-spo2

    // prp-eqp1
    // covered by prp-spo1 and scm-eqp1 program.addRule(new Rule(term(x, p2, y), term(p1, OWL_EQUIVALENT_PROPERTY, p2), term(x, p1, y)));

    // prp-eqp2
    // covered by prp-spo1 and scm-eqp1 program.addRule(new Rule(term(x, p1, y), term(p1, OWL_EQUIVALENT_PROPERTY, p2), term(x, p2, y)));

    // prp-pdw
    program.addRule(new Rule(FALSE, term(p1, OWL_DISJOINT_WITH, p2), term(x, p1, y), term(x, p2, y)));


    //TODO: prp-adp

    // prp-inv1
    program.addRule(new Rule(term(y, p1, x), term(p1, OWL_INVERSE_OF, p2), term(x, p2, y)));

    // prp-inv2
    program.addRule(new Rule(term(y, p2, x), term(p1, OWL_INVERSE_OF, p2), term(x, p1, y)));

    // TODO: prp-key

    // prp-npa1
    program.addRule(new Rule(FALSE, term(x, OWL_SOURCE_INDIVIDUAL, i1), term(x, OWL_ASSERTION_PROPERTY, p), term(x, OWL_TARGET_INDIVIDUAL, i2), term(i1, p, i2)));

    //  prp-npa2
    program.addRule(new Rule(FALSE, term(x, OWL_SOURCE_INDIVIDUAL, i1), term(x, OWL_ASSERTION_PROPERTY, p), term(x, OWL_TARGET_VALUE, i2), term(i1, p, i2)));


    // Table 6. The Semantics of Classes

    // cls-thing
    program.addRule(new Rule(term(OWL_THING, RDF_TYPE, OWL_CLASS)));

    // cls-nothing1
    program.addRule(new Rule(term(OWL_NOTHING, RDF_TYPE, OWL_CLASS)));

    // cls-nothing2
    program.addRule(new Rule(FALSE, term(x, RDF_TYPE, OWL_NOTHING)));

    //TODO: cls-int1

    // cls_int2
    program.addRule(new Rule(term(y, RDF_TYPE, ci), term(c, OWL_INTERSECTION_OF, x), term(x, LOG_HAS_ELEMENT, ci), term(y, RDF_TYPE, c)));

    // cls_uni
    program.addRule(new Rule(term(y, RDF_TYPE, c), term(c, OWL_UNION_OF, x), term(x, LOG_HAS_ELEMENT, ci), term(y, RDF_TYPE, ci)));

    // cls-com
    program.addRule(new Rule(FALSE, term(c1, OWL_COMPLEMENT_OF, c2), term(x, RDF_TYPE, c1), term(x, RDF_TYPE, c2)));

    // cls-svf1
    program.addRule(new Rule(term(u, RDF_TYPE, x), term(x, OWL_SOME_VALUES_FROM, y), term(x, OWL_ON_PROPERTY, p), term(u, p, v), term(v, RDF_TYPE, y)));

    // cls-svf2
    program.addRule(new Rule(term(u, RDF_TYPE, x), term(x, OWL_SOME_VALUES_FROM, OWL_THING), term(x, OWL_ON_PROPERTY, p), term(u, p, v)));

    // cls-acf
    program.addRule(new Rule(term(v, RDF_TYPE, y), term(x, OWL_ALL_VALUES_FROM, y), term(x, OWL_ON_PROPERTY, p), term(u, RDF_TYPE, x), term(u, p, v)));

    // cls-hv1
    program.addRule(new Rule(term(u, p, y), term(x, OWL_HAS_VALUE, y), term(x, OWL_ON_PROPERTY, p), term(u, RDF_TYPE, x)));

    // cls-hv2
    program.addRule(new Rule(term(u, RDF_TYPE, x), term(x, OWL_HAS_VALUE, y), term(x, OWL_ON_PROPERTY, p), term(u, p, y)));

    //TODO: mewCardinality rules other rules

    program.addRule(new Rule(term(y, RDF_TYPE, c), term(c, OWL_ONE_OF, x), term(x, LOG_HAS_ELEMENT, y)));


    // Table 7. The Semantics of Class Axioms
    // cax-sco
    program.addRule(new Rule(term(x, RDF_TYPE, c2), term(c1, RDFS_SUBCLASS_OF, c2), term(x, RDF_TYPE, c1)));

    // cax-eqc1
    // covered by cax-sco and scm-eqc1 program.addRule(new Rule(term(x, RDF_TYPE, c2), term(c1, OWL_EQUIVALENT_CLASS, c2), term(x, RDF_TYPE, c1)));

    // cax-eqc2
    // covered by cax-sco and scm-eqc1 program.addRule(new Rule(term(x, RDF_TYPE, c1), term(c1, OWL_EQUIVALENT_CLASS, c2), term(x, RDF_TYPE, c2)));

    // cax-dw
    program.addRule(new Rule(FALSE, term(c1, OWL_DISJOINT_WITH, c2), term(x, RDF_TYPE, c1), term(x, RDF_TYPE, c2)));

    //TODO: cat-adc


    //  Table 8. The Semantics of Datatypes
    // TODO


    // Table 9. The Semantics of Schema Vocabulary
    // scm-cls
    program.addRule(new Rule(term(c, RDFS_SUBCLASS_OF, c), term(c, RDF_TYPE, OWL_CLASS)));
    program.addRule(new Rule(term(c, OWL_EQUIVALENT_CLASS, c), term(c, RDF_TYPE, OWL_CLASS)));
    program.addRule(new Rule(term(c, RDFS_SUBCLASS_OF, OWL_THING), term(c, RDF_TYPE, OWL_CLASS)));
    program.addRule(new Rule(term(OWL_NOTHING, RDFS_SUBCLASS_OF, c), term(c, RDF_TYPE, OWL_CLASS)));

    // scm-sco
    program.addRule(new Rule(term(c1, RDFS_SUBCLASS_OF, c3), term(c1, RDFS_SUBCLASS_OF, c2), term(c2, RDFS_SUBCLASS_OF, c3)));

    // scm-eqc1
    program.addRule(new Rule(term(c1, RDFS_SUBCLASS_OF, c2), term(c1, OWL_EQUIVALENT_CLASS, c2)));
    program.addRule(new Rule(term(c2, RDFS_SUBCLASS_OF, c1), term(c1, OWL_EQUIVALENT_CLASS, c2)));

    // scm-eqc2
    program.addRule(new Rule(term(c1, OWL_EQUIVALENT_CLASS, c2), term(c1, RDFS_SUBCLASS_OF, c2), term(c2, RDFS_SUBCLASS_OF, c1)));

    // scm-op
    program.addRule(new Rule(term(p, RDFS_SUB_PROPERTY_OF, p), term(p, RDF_TYPE, OWL_OBJECT_PROPERTY)));

    // scm-dp
    program.addRule(new Rule(term(p, OWL_EQUIVALENT_PROPERTY, p), term(p, RDF_TYPE, OWL_DATA_PROPERTY)));

    // scm-spo
    program.addRule(new Rule(term(p1, RDFS_SUB_PROPERTY_OF, p3), term(p1, RDFS_SUB_PROPERTY_OF, p2), term(p2, RDFS_SUB_PROPERTY_OF, p3)));

    // scm-eqp1
    program.addRule(new Rule(term(p1, RDFS_SUB_PROPERTY_OF, p2), term(p1, OWL_EQUIVALENT_PROPERTY, p2)));
    program.addRule(new Rule(term(p2, RDFS_SUB_PROPERTY_OF, p1), term(p1, OWL_EQUIVALENT_PROPERTY, p2)));

    // scm-eqp2
    program.addRule(new Rule(term(p1, OWL_EQUIVALENT_PROPERTY, p2), term(p1, RDFS_SUB_PROPERTY_OF, p2), term(p2, RDFS_SUB_PROPERTY_OF, p1)));

    // scm-dom1
    program.addRule(new Rule(term(p, RDFS_DOMAIN, c2), term(p, RDFS_DOMAIN, c1), term(c1, RDFS_SUBCLASS_OF, c2)));

    // scm-dom2
    program.addRule(new Rule(term(p2, RDFS_DOMAIN, c), term(p2, RDFS_DOMAIN, c), term(p1, RDFS_SUB_PROPERTY_OF, p2)));

    // scm-rng1
    program.addRule(new Rule(term(p, RDFS_RANGE, c2), term(p, RDFS_RANGE, c1), term(c1, RDFS_SUBCLASS_OF, c2)));

    // scm-rng2
    program.addRule(new Rule(term(p2, RDFS_RANGE, c), term(p2, RDFS_RANGE, c), term(p1, RDFS_SUB_PROPERTY_OF, p2)));

    //TODO: scm-int and scm-uni

    return program;
  }

  private class ProgramRenderer extends RDFRendererBase {

    private final Program program = new Program();
    private final Set<RDFResource> seen = new HashSet<>();

    ProgramRenderer(OWLOntology ontology) {
      super(ontology);
    }

    public Program getProgram() {
      return program;
    }

    @Override
    protected void beginDocument() {
    }

    @Override
    protected void endDocument() {
    }

    @Override
    protected void writeAnnotationPropertyComment(OWLAnnotationProperty prop) {
    }

    @Override
    protected void writeDataPropertyComment(OWLDataProperty prop) {
    }

    @Override
    protected void writeObjectPropertyComment(OWLObjectProperty prop) {
    }

    @Override
    protected void writeClassComment(OWLClass cls) {
    }

    @Override
    protected void writeDatatypeComment(OWLDatatype datatype) {
    }

    @Override
    protected void writeIndividualComments(OWLNamedIndividual ind) {
    }

    @Override
    protected void writeBanner(String name) {
    }

    @Override
    protected void render(RDFResource node, boolean root) {
      if (seen.contains(node)) {
        return;
      }
      seen.add(node);

      getRDFGraph().getTriplesForSubject(node).forEach(triple -> {
        program.addRule(new Rule(tupleSerializer.convertTriple(triple.getSubject(), triple.getPredicate(), triple.getObject())));
        if (triple.getObject() instanceof RDFResource) {
          render((RDFResource) triple.getObject(), false);
        }
      });
    }
  }
}
