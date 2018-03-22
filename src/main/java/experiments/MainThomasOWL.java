package experiments;

import bashlog.BashlogCompiler;
import common.parser.ParserReader;
import common.parser.Program;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import owl.OntologyConverter;

import java.io.File;

public class MainThomasOWL {

  private static String lubmScript(String lubmDir) {
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

  public static Program lubmProgram(String lubmDir, String queryDir) throws Exception {
    Program lubmProgram = Program.merge(lubmTBox(), Program.loadFile(queryDir + "/queries.txt"));
    String script = lubmScript(lubmDir);
    lubmProgram.addRules(Program.read(new ParserReader(script)));
    return lubmProgram;
  }

  private static Program lubmTBox() throws Exception {
    OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
    IRI ontologyIRI = IRI.create("http://swat.cse.lehigh.edu/onto/univ-bench.owl");
    ontologyManager.loadOntology(ontologyIRI);
    OntologyConverter converter = new OntologyConverter();
    return converter.convert(ontologyManager.getOntology(ontologyIRI));
  }

  public static void main(String[] args) throws Exception {
    Program p = lubmProgram("data/lubm/1/", "data/lubm");
    System.out.println(p);

    long start = System.currentTimeMillis();
    System.out.println(BashlogCompiler.compileQuery(p, "query1/1"));
    System.out.println("end: " + (System.currentTimeMillis() - start) / 1000);
  }
}
