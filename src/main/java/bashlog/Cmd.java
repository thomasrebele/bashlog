package bashlog;

import java.io.IOException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import common.parser.Program;

/** Command line program to translate a bashlog datalog program to a bash script. */
public class Cmd {

  /** Command line arguments */
  public static class Args {

    @Parameter(names = { "--help", "-h" }, description = "help")
    public boolean help;

    @Parameter(names = { "--plan" }, description = "print plan")
    public boolean debug;

    @Parameter(names = "--query-file", description = "Bashdatalog query file (containing datalog rules)")
    private String queryFile;

    @Parameter(names = "--query-pred", description = "query predicate (which predicate to evaluate)")
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
    if (argv.length == 0 && args.help) {
      cmd.usage();
      return;
    }

    // translate/compile
    Program p = Program.loadFile(args.queryFile, BashlogCompiler.BASHLOG_PARSER_FEATURES);
    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, args.queryPredicate);
    try {
      String bash = bc.compile("", false);
      if (args.debug) {
        System.out.println(bc.debugInfo());
      }
      System.out.println(bash);
    } catch (Exception e) {
      System.out.println(bc.debugInfo());
      throw (e);
    }
  }


}
