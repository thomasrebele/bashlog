package rdf;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntologyManager;

public class MainOwl {
  public static void main(String[] args) throws Exception {
    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    IRI ontologyIRI = IRI.create("http://swat.cse.lehigh.edu/onto/univ-bench.owl");
    ontologyManager.loadOntologyFromOntologyDocument(ontologyIRI);

    OWL2RLOntologyConverter converter = new OWL2RLOntologyConverter(new RDFTripleTupleSerializer("fact"));
    converter.convert(ontologyManager.getOntology(ontologyIRI)).rules().forEach(rule ->
            System.out.println(rule.toString())
    );
  }
}
