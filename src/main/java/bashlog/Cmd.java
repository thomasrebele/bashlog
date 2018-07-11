package bashlog;

import java.io.IOException;
import java.util.Set;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import common.parser.Program;

/** Command line program to translate a bashlog datalog program to a bash script. */
public class Cmd {

  /** Command line arguments */
  public static class Args {

    @Parameter(names = { "--help", "-h" }, description = "help", help=true, hidden=true)
    public boolean help;

    @Parameter(names = { "--plan" }, description = "print plan")
    public boolean debug;

    @Parameter(names = "--query-file", description = "a Bash Datalog query file, which contains datalog rules")
    private String queryFile;

    @Parameter(names = "--query-pred", description = "the predicate that should be evaluated")
    private String queryPredicate;
  }

  public static void main(String[] argv) throws IOException {
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
    Set<String> features = BashlogCompiler.BASHLOG_PARSER_FEATURES;
    Program p = Program.loadFile(args.queryFile, features);
    
    String queryPred = args.queryPredicate;
    queryPred = p.searchRelation(queryPred);
    if (queryPred == null || queryPred.trim().isEmpty()) {
      queryPred = p.rules().get(p.rules().size() - 1).head.getRelation();
    }

    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, queryPred);
    try {
      if(args.debug) {
        bc.enableDebug();
      }
      String bash = bc.compile("", "", false);
      System.out.println(bash);
      if (args.debug) {
        System.out.println(bc.debugInfo());
      }
    } catch (Exception e) {
      System.out.println(bc.debugInfo());
      throw (e);
    }
  }


}
