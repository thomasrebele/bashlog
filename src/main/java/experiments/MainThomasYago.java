package experiments;

import common.parser.Program;
import sqllog.SqllogCompiler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;

import bashlog.BashlogCompiler;

public class MainThomasYago {

  public static Program yagoProgram(String queryDir) throws IOException {
    return Program.merge(
            Program.loadFile(queryDir + "/full_tbox.txt"),
            Program.loadFile(queryDir + "/full_queries.txt")
    );
  }

  public static void main(String[] args) throws IOException {

    String scriptDir = "experiments/edbt2017/yago/bashlog/";
    String sqlDir = "experiments/edbt2017/yago/sql/";
    new File(scriptDir).mkdirs();
    new File(sqlDir).mkdirs();

    for (int i = 5; i <= 5; i++) {
      Program p = yagoProgram("data/yago");
      String relation = "query" + i + "/1";
      try {
        String script = BashlogCompiler.compileQuery(p, relation);

        Program sqlProg = new Program();
        p.rules().forEach(r -> {
          if (r.body.size() == 1 && "bash_command".equals(r.body.get(0).name)) {
            // ignore this rule
          } else {
            sqlProg.addRule(r);
          }
        });

        Files.write(Paths.get(scriptDir + "query" + i + ".sh"), script.getBytes());
        /*String sql = new SqllogCompiler().compile(sqlProg, new HashSet<>(Arrays.asList("allFacts/4")), relation);
        Files.write(Paths.get(sqlDir + "query" + (i + 1) + ".sql"), sql.getBytes());*/
      } catch (Exception e) {
        throw new RuntimeException("in query " + i, e);
      }
    }
  }
}
