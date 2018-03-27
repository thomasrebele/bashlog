package experiments.lubm;

import bashlog.BashlogCompiler;
import common.parser.*;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import owl.OntologyConverter;
import owl.OntologyConverterTriple;
import sparqlog.SparqlogCompiler;
import sqllog.SqllogCompiler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

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
  public static String lubmScript3(String lubmDir, Program p) {
    StringBuilder sb = new StringBuilder();
    sb.append("allFacts(X,Y,Z) :~ cat ").append(lubmDir).append("/all\n");
    for (String rel : p.allRelations()) {
      if (rel.startsWith("query")) continue;
      String[] tmp = rel.split("/");
      if ("1".equals(tmp[1])) {
        sb.append(tmp[0] + "(X) :- allFacts(X,\"rdf:type\", \"").append(tmp[0]).append("\").\n");
      } else if ("2".equals(tmp[1])) {
        sb.append(tmp[0] + "(X, Y) :- allFacts(X,\"").append(tmp[0]).append("\", Y).\n");
      }
    }

    return sb.toString();
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
    String script = lubmScript3(lubmDir, lubmProgram);
    lubmProgram.addRules(Program.read(new ParserReader(script)));
    return lubmProgram;
  }

  private static Program ontologyProgram;

  /** LUBM queries for 3-column TSV */
  public static Program lubmProgramOWL3(String lubmDir, String queryDir) throws IOException {
    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    IRI ontologyIRI = IRI.create("file:data/lubm/univ-bench.owl");

    if (ontologyProgram == null) {
      OWLOntology ontology;
      try {
        ontology = ontologyManager.loadOntology(ontologyIRI);
      } catch (OWLOntologyCreationException e) {
        throw new IOException(e);
      }
      OntologyConverter converter = new OntologyConverter();
      ontologyProgram = converter.convert(ontology);
    }

    Program lubmProgram = Program.merge(ontologyProgram, Program.loadFile(queryDir + "/queries.txt"));
    String script = lubmScript3(lubmDir, lubmProgram);
    lubmProgram.addRules(Program.read(new ParserReader(script)));
    return lubmProgram;
  }

  /** LUBM queries for 3-column TSV */
  public static Program lubmProgramOWLRL(String lubmDir, String queryDir) throws IOException {
    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    IRI ontologyIRI = IRI.create("file:data/lubm/univ-bench.owl");

    if (ontologyProgram == null) {
      OWLOntology ontology;
      try {
        ontology = ontologyManager.loadOntology(ontologyIRI);
      } catch (OWLOntologyCreationException e) {
        throw new IOException(e);
      }
      OntologyConverterTriple converter = new OntologyConverterTriple("allFacts");
      ontologyProgram = normalizeProgram(converter.convert(ontology));
    }

    Program lubmProgram = Program.merge(ontologyProgram, Program.loadFile(queryDir + "/queries.txt"));
    String script = lubmScript3(lubmDir, lubmProgram);
    lubmProgram.addRules(Program.read(new ParserReader(script)));
    return lubmProgram;
  }

  private static Program normalizeProgram(Program program) {
    return new Program(program.rules().stream().map(rule ->
            new Rule(
                    normalizeCompoundTerm(rule.head),
                    rule.body.stream().map(BashlogLUBM::normalizeCompoundTerm).collect(Collectors.toList()))
    ));
  }

  private static CompoundTerm normalizeCompoundTerm(CompoundTerm compoundTerm) {
    return new CompoundTerm(compoundTerm.name, compoundTerm.negated, Arrays.stream(compoundTerm.args).map(term -> {
      if(term instanceof Constant) {
        Object v = ((Constant) term).getValue();
        if(v instanceof String) {
          return new Constant<>(((String) v).replace("http://swat.cse.lehigh.edu/onto/univ-bench.owl#", "").replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"));
        }
      }
      return term;
    }).toArray(Term[]::new));
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

        /*String sql = new SqllogCompiler().compile(sqlProg, new HashSet<>(Collections.singletonList("allFacts/3")), relation);
        String noDBSql = new SqllogCompiler(true, true).compile(sqlProg, new HashSet<>(Arrays.asList("allFacts/3")), relation);
        Files.write(Paths.get(scriptDir + "query" + (i + 1) + ".sh"), script.getBytes());
        Files.write(Paths.get(sqlDir + "query" + (i + 1) + ".sql"), sql.getBytes());
        Files.write(Paths.get(nodbSqlDir + "query" + (i + 1) + ".sql"), noDBSql.getBytes());*/

        try {
        String sparqlog = new SparqlogCompiler().compile(p, relation);
          Files.write(Paths.get(sparqlogDir + "query" + (i + 1) + ".sparql"), sparqlog.getBytes());
        } catch (UnsupportedOperationException e) {
          Files.write(Paths.get(sparqlogDir + "unsupported-query" + (i + 1) + ".sparql"), "".getBytes());
        }
      } catch (Exception e) {
        throw new RuntimeException("in query " + (i + 1), e);
      }
    }
  }

}
