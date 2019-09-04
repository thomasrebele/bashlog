package rdf;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.rdf.api.*;

import common.parser.*;

/**
 * Serializes RDF triples in tuples like tripleTupleName(subject, predicate, object) with optional IRI shortening
 */
public class RDFTripleTupleSerializer implements RDFTupleSerializer {

  private final String tripleTupleName;
  private final Map<String,String> prefixes;

  public RDFTripleTupleSerializer(String tripleTupleName, Map<String,String> prefixes) {
    this.tripleTupleName = tripleTupleName;
    this.prefixes = prefixes;
  }

  public RDFTripleTupleSerializer(String tripleTupleName) {
    this(tripleTupleName, Collections.emptyMap());
  }

  public CompoundTerm convertTriple(Term subject, Term predicate, Term object) {
    return new CompoundTerm(tripleTupleName, subject, predicate, object);
  }

  public Term convertTerm(RDFTerm term) {
    if (term instanceof Literal) {
      return new Constant<>(((Literal) term).getLexicalForm());
    } else if (term instanceof BlankNode) {
      return new Constant<>(((BlankNode) term).uniqueReference());
    } else if (term instanceof IRI) {
      return new Constant<>(shortened(((IRI) term).getIRIString()));
    } else {
      throw new IllegalArgumentException("Not supported RDF term: " + term);
    }
  }

  private String shortened(String IRI) {
    for(Map.Entry<String,String> prefix : prefixes.entrySet()) {
      if(IRI.startsWith(prefix.getKey())) {
        return prefix.getValue() + IRI.substring(prefix.getKey().length());
      }
    }
    return "<" + IRI + ">";
  }
}
