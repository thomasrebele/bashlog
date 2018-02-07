package experiments;

import common.parser.Program;

import java.io.IOException;

import bashlog.BashlogCompiler;

public class MainThomasWikidata {

  public static Program wikidataProgram(String queryDir) throws IOException {
    return Program.merge(
            Program.loadFile(queryDir + "/full_tbox.txt"),
            Program.loadFile(queryDir + "/full_queries.txt")
    );
  }

  public static void main(String[] args) throws Exception {
    Program p = wikidataProgram("data/wikidata");

    long start = System.currentTimeMillis();
    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, "query2/1");
    System.out.println(bc.debugInfo());
    System.out.println(bc.compile());
    System.out.println("end: " + (System.currentTimeMillis() - start) / 1000);
  }
}
