package experiments;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import bashlog.BashlogCompiler;
import common.parser.Program;
import experiments.lubm.BashlogLUBM;

public class MainThomas {

  public static void main(String[] args) throws IOException {
    /*System.out.println("press key");
    System.in.read();*/
    profile();
  }

  public static void profile() throws IOException {
    // reach
    /*Program p = new Program().loadFile("experiments/edbt2017/reach/bashlog/query-com-orkut.ungraph.txt.gz.txt");
    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, "reach/1");
    System.out.println(bc.compile());*/

    // lubm

    String scriptDir = "experiments/edbt2017/lubm/bashlog/";
    new File(scriptDir).mkdirs();
    Program p = BashlogLUBM.lubmProgram3("~/extern/data/bashlog/lubm/$1/", "data/lubm");
    String relation = BashlogLUBM.queries[1];
    //relation = "Student/1";
    //relation = "takesCourse/2";
    //relation = BashlogLUBM.queries[2];
    //for (int i = 1; i <= 14; i++) {
    for (int i = 9; i <= 9; i++) {
      relation = BashlogLUBM.queries[i - 1];
      p.rulesForRelation(relation).forEach(r -> System.out.println(r));
      BashlogCompiler bc = BashlogCompiler.prepareQuery(p, relation);
      
      try {
        String script = bc.compile();
        Files.write(new File("/home/tr/tmp/bashlog/new_query" + i).toPath(), script.getBytes());
        System.out.println(bc.debugInfo());
        System.out.println(script);
      } catch (Exception e) {
        System.err.println("problem with lubm query " + i);
        e.printStackTrace();
      }

      /*SqllogCompiler sc = new SqllogCompiler(true, true);
      Program sqlProg = new Program();
      p.rules().forEach(r -> {
        if (r.body.size() == 1 && "bash_command".equals(r.body.get(0).name)) {
          // ignore this rule
        } else {
          sqlProg.addRule(r);
        }
      });
      System.out.println(sc.compile(sqlProg, new HashSet<>(Arrays.asList("allFacts/3")), relation));*/
    }

    //--------------------------------------------------------------------------------

    /*String code = "rel(X,Y) :~ cat !old/2 \n" + //
        "old(X,Y) :~ cat abc.txt \n";
    p = Program.read(new ParserReader(code));
    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, "rel");
    String bash = bc.compile();
    System.out.println(bash);
    System.out.println(bc.debugInfo());*/
  }
}
