package rdf;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import common.parser.Program;

public class MainOwl {
  public static void main(String[] args) throws Exception {
    RDFTupleSerializer serializer = new RDFSpecificTuplesSerializer(Collections.singletonMap(
            "http://swat.cse.lehigh.edu/onto/univ-bench.owl#", ""
    ));

    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    IRI ontologyIRI = IRI.create("http://swat.cse.lehigh.edu/onto/univ-bench.owl");
    ontologyManager.loadOntologyFromOntologyDocument(ontologyIRI);

    OntologyConverter owlConverter = new OntologyConverter(serializer);
    Program ontologyProgram = owlConverter.convert(ontologyManager.getOntology(ontologyIRI));

    SPARQLConverter sparqlConverter = new SPARQLConverter(serializer);
    int i = 1;
    for(String query : new String(Files.readAllBytes(Paths.get( "data/lubm/queries.sparql"))).split("\n\n")) {
      Program program = sparqlConverter.convert(query, "query" + (i++));
      if(i == 8) {
        System.out.println(query);
        System.out.println(program);
        return;
      }
    }
  }
}
