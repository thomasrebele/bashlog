package bashlog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import common.parser.Program;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Cmd {

  public static void main(String[] argv) throws IOException {
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

    Program p = Program.loadFile(args.queryFile);
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

  public static class Args {

    @Parameter(names = {"--help", "-h"}, description = "help")
    public boolean help;

    // @Parameter(names = { "-log", "-verbose" }, description = "Level of
    // verbosity")
    // private Integer verbose = 1;
    @Parameter(names = {"--plan"}, description = "print plan")
    public boolean debug;
    @Parameter
    private List<String> parameters = new ArrayList<>();
    @Parameter(names = "--query-file", description = "Bashdatalog query file")
    private String queryFile;
    @Parameter(names = "--query-pred", description = "query predicate")
    private String queryPredicate;
  }
}
