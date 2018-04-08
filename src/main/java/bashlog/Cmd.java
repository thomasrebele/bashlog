package bashlog;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    if (argv.length == 0) {
      argv = new String[] { "--query-file", "data/lubm/bashlog-all.txt", "--query-pred", "query1/" };
    }

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
    Set<String> features = BashlogCompiler.BASHLOG_PARSER_FEATURES;
    Program p = Program.loadFile(args.queryFile, features);

    BashlogCompiler bc = BashlogCompiler.prepareQuery(p, args.queryPredicate);
    try {
      String bash = bc.compile("", "", false);
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
