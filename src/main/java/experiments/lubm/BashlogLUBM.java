package experiments.lubm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;

import bashlog.BashlogCompiler;
import common.DatalogTools;
import common.parser.*;
import rdf.*;

public class BashlogLUBM {

  public static String[] queries = new String[] {
      "query1/1",
      "query2/3",
      "query3/1",
      "query4/4",
      "query5/1",
      "query6/1",
      "query7/2",
      "query8/3",
      "query9/3",
      "query10/1",
      "query11/1",
      "query12/2",
      "query13/1",
      "query14/1" };

  /** LUBM queries for 2-column TSV */
  public static String lubmScript2(String lubmDir) {
    StringBuilder sb = new StringBuilder();
    File dir = new File(lubmDir);
    for (File f : dir.listFiles()) {
      int cols = 1;
      if (Character.isLowerCase(f.getName().charAt(0))) {
        cols = 2;
      }
      sb.append(f.getName()).append("(");
      for (int i = 0; i < cols; i++) {
        if (i > 0) sb.append(", ");
        sb.append((char) ('X' + i));
      }
      sb.append(") :~ cat ").append(lubmDir).append(f.getName()).append("\n");
    }

    return sb.toString();
  }

  /** LUBM queries for 3-column TSV */
  public static Program lubmInputRules3(String lubmDir, Program p) {
    Program result = new Program();
    result.addRule(Rule.read(new ParserReader("allFacts(X,Y,Z) :~ cat " + lubmDir + "/all\n"), BashlogCompiler.BASHLOG_PARSER_FEATURES));
    result.addRules(DatalogTools.inputRules3("allFacts", p, "rdf:type", false));
    return result;
  }

  /** LUBM queries for 2-column TSV */
  public static Program lubmProgram2(String lubmDir, String queryDir) throws IOException {
    Program lubmProgram = Program.merge(Program.loadFile(queryDir + "/tbox.txt"), Program.loadFile(queryDir + "/queries.txt"));
    String script = lubmScript2(lubmDir);
    lubmProgram.addRules(Program.read(new ParserReader(script)));
    return lubmProgram;
  }

  /** LUBM queries for 3-column TSV */
  public static Program lubmProgram3(String lubmDir, String queryDir) throws IOException {
    Program lubmProgram = Program.merge(Program.loadFile(queryDir + "/tbox.txt"), Program.loadFile(queryDir + "/queries.txt"));
    lubmProgram.addRules(lubmInputRules3(lubmDir, lubmProgram));
    return lubmProgram;
  }


  /** LUBM queries for 3-column TSV */
  public static Program lubmProgramOWLSparql3(String lubmDir, String queryDir) throws IOException {
    RDFTupleSerializer tupleSerializer = new RDFSpecificTuplesSerializer(
        Collections.singletonMap("http://swat.cse.lehigh.edu/onto/univ-bench.owl#", ""));
    return lubmProgramOWL3(lubmDir, lubmSPARQLProgram(queryDir, tupleSerializer));
  }

  public static Program lubmProgramOWLDatalog3(String lubmDir, String queryDir) throws IOException {
    return lubmProgramOWL3(lubmDir, Program.loadFile(queryDir + "/queries.txt"));
  }

  /** LUBM queries for 3-column TSV */
  private static Program lubmProgramOWL3(String lubmDir, Program query) throws IOException {
    RDFTupleSerializer tupleSerializer = new RDFSpecificTuplesSerializer(Collections.singletonMap(
            "http://swat.cse.lehigh.edu/onto/univ-bench.owl#", ""
    ));

    try {
      OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
      IRI ontologyIRI = IRI.create("file:data/lubm/univ-bench.owl");
      OWLOntology ontology = ontologyManager.loadOntology(ontologyIRI);
      OntologyConverter converter = new OntologyConverter(tupleSerializer);
      Program ontologyProgram = converter.convert(ontology);

      Program lubmProgram = Program.merge(ontologyProgram, query);
      lubmProgram.addRules(lubmInputRules3(lubmDir, ontologyProgram));
      return lubmProgram;
    } catch (OWLOntologyCreationException e) {
      throw new IOException(e);
    }
  }

  /** LUBM queries for 3-column TSV */
  public static Program lubmProgramOWLRL(String lubmDir, String queryDir) throws IOException {
    RDFTupleSerializer tupleSerializer = new RDFTripleTupleSerializer("allFacts");
    try {
      OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
      IRI ontologyIRI = IRI.create("file:data/lubm/univ-bench.owl");
      OWLOntology ontology = ontologyManager.loadOntology(ontologyIRI);
      OWL2RLOntologyConverter converter = new OWL2RLOntologyConverter(tupleSerializer);
      Program ontologyProgram = converter.convert(ontology);

      Program lubmProgram = Program.merge(ontologyProgram, lubmSPARQLProgram(queryDir, tupleSerializer));
      lubmProgram.addRules(lubmInputRules3(lubmDir, ontologyProgram));
      return lubmProgram;
    } catch (OWLOntologyCreationException e) {
      throw new IOException(e);
    }
  }

  private static Program lubmSPARQLProgram(String queryDir, RDFTupleSerializer tupleSerializer) throws IOException {
    SPARQLConverter sparqlConverter = new SPARQLConverter(tupleSerializer);
    Program program = new Program();
    int i = 1;
    for(String query : new String(Files.readAllBytes(Paths.get(queryDir + "/queries.sparql"))).split("\n\n")) {
      Program p = sparqlConverter.convert(query, "query" + (i++));
      System.out.println(p);
      program.addRules(p);
    }
    return program;
  }

  public static void main(String[] args) throws IOException {

    String scriptDir = "experiments/edbt2017/lubm/bashlog/";
    String sqlDir = "experiments/edbt2017/lubm/sql/";
    String nodbSqlDir = "experiments/edbt2017/lubm/nodb_sql/";
    String sparqlogDir = "experiments/edbt2017/lubm/sparqlog/";
    new File(scriptDir).mkdirs();
    new File(sqlDir).mkdirs();
    new File(nodbSqlDir).mkdirs();
    for (int i = 0; i < 14; i++) {
      Program p = lubmProgram3("~/extern/data/bashlog/lubm/$1/", "data/lubm");
      String relation = queries[i];
      try {
        String script = BashlogCompiler.compileQuery(p, relation);

        Program sqlProg = new Program();
        p.rules().forEach(r -> {
          if (r instanceof BashRule) {
            // ignore this rule
          } else {
            sqlProg.addRule(r);
          }
        });
        Files.write(Paths.get(scriptDir + "query" + (i + 1) + ".sh"), script.getBytes());

        /*String sql = new SqllogCompiler().compile(sqlProg, new HashSet<>(Collections.singletonList("allFacts/3")), relation);
        String noDBSql = new SqllogCompiler(true, true).compile(sqlProg, new HashSet<>(Arrays.asList("allFacts/3")), relation);
        Files.write(Paths.get(sqlDir + "query" + (i + 1) + ".sql"), sql.getBytes());
        Files.write(Paths.get(nodbSqlDir + "query" + (i + 1) + ".sql"), noDBSql.getBytes());*/

        /*try {
        String sparqlog = new SparqlogCompiler().compile(p, relation);
          Files.write(Paths.get(sparqlogDir + "query" + (i + 1) + ".sparql"), sparqlog.getBytes());
        } catch (UnsupportedOperationException e) {
          Files.write(Paths.get(sparqlogDir + "unsupported-query" + (i + 1) + ".sparql"), "".getBytes());
        }*/
      } catch (Exception e) {
        throw new RuntimeException("in query " + (i + 1), e);
      }
    }
  }

}
