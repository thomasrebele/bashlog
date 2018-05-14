package rdf;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.Term;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDFTerm;

import java.util.Map;

/**
 * Serializes RDF triples in tuples like predicate(subject, object) or type(subject)
 */
public class RDFSpecificTuplesSerializer implements RDFTupleSerializer {


  private final Map<String,String> prefixes;
  private final String rdfType;

  public RDFSpecificTuplesSerializer(Map<String,String> prefixes) {
    this.prefixes = prefixes;
    rdfType = shortened("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

  }

  @Override
  public CompoundTerm convertTriple(Term subject, Term predicate, Term object) {
    if(predicate instanceof Constant) {
      Object predicateValue = ((Constant) predicate).getValue();
      if(rdfType.equals(predicateValue)) {
        if(object instanceof Constant) {
          Object value = ((Constant) object).getValue();
          if(value instanceof String) {
            return new CompoundTerm((String) value, subject);
          }
        }
        throw new IllegalArgumentException("The range of rdf:type is owl:Class and should be bounded: <" + subject + ", " + predicate + ", " + object + ">");
      } else if(predicateValue instanceof String) {
        return new CompoundTerm((String) predicateValue, subject, object);
      }
    }
    throw new UnsupportedOperationException("The predicate of the triple should be a string constant: " + subject + " " + predicate + " " + object);
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
