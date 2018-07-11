package bashlog;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import common.DatalogTools;
import common.parser.BashRule;
import common.parser.ParserReader;
import common.parser.Program;
import rdf.OntologyConverter;
import rdf.RDFTripleTupleSerializer;
import rdf.RDFTupleSerializer;
import rdf.SPARQLConverter;

/** Command line program to translate a SPARQL / OWL query to a bash script. */
public class CmdSparql {

  /** Command line arguments */
  public static class Args {

    @Parameter(names = { "--help", "-h" }, description = "help", help = true, hidden = true)
    public boolean help;

    @Parameter(names = { "--plan" }, description = "print plan")
    public boolean debug;

    @Parameter(names = "--sparql", description = "a file containing the SPARQL query")
    private String sparql;

    @Parameter(names = "--owl", description = "a file containing the OWL ontology", required = false)
    private String owl;

    @Parameter(names = "--ntriples", description = "a file containing the N-Triples", required = false)
    private String ntriples;

    @Parameter(names = "--debug-algebra", description = "print algebra plan after each optimization step", required = false)
    private boolean debugAlgebra;
    
    @Parameter(names = "--debug-datalog", description = "print Datalog program", required = false)
    private boolean debugDatalog;
  }

  public static void main(String[] argv) throws IOException, OWLOntologyCreationException {

    // parse arguments
    Args args = new Args();
    JCommander cmd = JCommander.newBuilder().addObject(args).build();
    cmd.parse(argv);
    for (String str : cmd.getUnknownOptions()) {
      System.out.println("warning: unknown option " + str + "");
    }
    if (argv.length == 0 || args.help) {
      cmd.usage();
      return;
    }

    // translate/compile

    String helperPred = "api_tmp_";
    String queryPred = helperPred + "query";
    String factPred = helperPred + "facts";
    String rdfTypeConst = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

    RDFTupleSerializer tupleSerializer = new RDFTripleTupleSerializer(helperPred + "facts", Collections.emptyMap());

    Program query;
    String bashlog = null, sparql = null;
    // convert sparql
    SPARQLConverter sparqlConverter = new SPARQLConverter(tupleSerializer, helperPred);
    sparql = new String(Files.readAllBytes(Paths.get(args.sparql)));
    query = sparqlConverter.convert(sparql, queryPred);

    // convert OWL
    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    String ontologyCode = new String(Files.readAllBytes(Paths.get(args.owl)));

    OWLOntology ontology;
    ontology = ontologyManager.loadOntologyFromOntologyDocument(new ByteArrayInputStream(ontologyCode.getBytes(StandardCharsets.UTF_8)));

    OntologyConverter converter = new OntologyConverter(tupleSerializer);
    Program ontologyProgram = converter.convert(ontology);
    // add owl rules to query
    query = Program.merge(ontologyProgram, query);

    // add input rules to query
    List<String> inputRelations = query.allRelations().stream()//
        .filter(s -> !s.startsWith(helperPred)).collect(Collectors.toList());
    Program input = DatalogTools.inputRules3(factPred, inputRelations, rdfTypeConst, true);
    query.addRules(input);

    // ntriple filename
    String filename = args.ntriples == null ? filename = "\"$@\"" : args.ntriples;
    String inputRule = factPred + "(X,Y,Z) :~ read_ntriples " + filename;
    query.addRule(BashRule.read(new ParserReader(inputRule), BashlogCompiler.BASHLOG_PARSER_FEATURES));
    
    queryPred = query.searchRelation(queryPred);
    BashlogCompiler preparedQuery = BashlogCompiler.prepareQuery(query, queryPred);
    if (args.debugAlgebra) {
      preparedQuery.enableDebug();
    }
    bashlog = preparedQuery.compile();
    
    if(args.debugDatalog) {
      bashlog += "\n\n# " + query.toStringOrdered(queryPred).replace("\n", "\n# ");
    }
    if (args.debugAlgebra) {
      bashlog += "\n\n" + preparedQuery.debugInfo();
    }
    System.out.println(bashlog);
  }

}
