package rdf;

import common.parser.CompoundTerm;
import common.parser.Term;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;

public interface RDFTupleSerializer {
  CompoundTerm convertTriple(Term subject, Term predicate, Term object);

  default CompoundTerm convertTriple(Term subject, IRI predicate, Term object) {
    return convertTriple(subject, convertTerm(predicate), object);
  }

  default CompoundTerm convertTriple(Term subject, IRI predicate, RDFTerm object) {
    return convertTriple(subject, predicate, convertTerm(object));
  }

  default CompoundTerm convertTriple(BlankNodeOrIRI subject, IRI predicate, Term object) {
    return convertTriple(convertTerm(subject), predicate, object);
  }

  default CompoundTerm convertTriple(BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {
    return convertTriple(convertTerm(subject), predicate, convertTerm(object));
  }

  Term convertTerm(RDFTerm term);
}
