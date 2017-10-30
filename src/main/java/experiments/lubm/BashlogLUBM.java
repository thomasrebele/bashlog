package experiments.lubm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import bashlog.BashlogEvaluator;
import common.parser.ParserReader;
import common.parser.Program;

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
    sb.append("all(X,Y,Z) :~ cat ").append(lubmDir).append("/all\n");
    for (String rel : p.allRelations()) {
      if (rel.startsWith("query")) continue;
      String[] tmp = rel.split("/");
      if ("1".equals(tmp[1])) {
        sb.append(tmp[0] + "(X) :- all(X,\"rdf:type\", \"").append(tmp[0]).append("\").\n");
      } else if ("2".equals(tmp[1])) {
        sb.append(tmp[0] + "(X, Y) :- all(X,\"").append(tmp[0]).append("\", Y).\n");
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

  public static void main(String[] args) throws IOException {
    

    String scriptDir = "experiments/edbt2017/lubm/bashlog/";
    new File(scriptDir).mkdirs();
    for (int i = 0; i < 14; i++) {
      Program p = lubmProgram3("~/extern/data/bashlog/lubm/$1/", "data/lubm");
      String relation = queries[i];
      try {
      String script = BashlogEvaluator.compileQuery(p, relation);
      Files.write(Paths.get(scriptDir + "query" + (i + 1) + ".sh"), script.getBytes());
      } catch (Exception e) {
        throw new RuntimeException("in query " + (i + 1), e);
      }
    }
  }

}
