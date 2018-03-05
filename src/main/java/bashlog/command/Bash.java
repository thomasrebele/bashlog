package bashlog.command;

import java.util.ArrayList;
import java.util.List;

import common.plan.node.PlanNode;

/**
 * Represent simple Bash scripts
 */
public interface Bash {

  public void generate(AutoIndent sb);

  public default String generate() {
    AutoIndent sb = new AutoIndent();
    generate(sb);
    return sb.generate();
  }

  /** A bash command (and possibly arguments) */
  public static class Command implements Bash {

    String cmd; // command

    List<Bash> args = new ArrayList<>(); // arguments

    List<Heredoc> docs = new ArrayList<>();

    public Command(String cmd) {
      this.cmd = cmd;
    }

    public Command file(String path) {
      args.add(new BashFile(path));
      return this;
    }

    /** Add the bash snippet as an argument. Creates a process substitution if necessary. */
    public Command file(Bash b) {
      if (b instanceof BashFile) {
        args.add(b);
      } else {
        args.add(b.wrap("<(", ")"));
      }
      return this;
    }

    public Command arg(String text) {
      if (text.length() == 0) return this;
      args.add(new Other(text));
      return this;
    }

    public void heredoc(Heredoc doc) {
      arg("<<" + doc.eof);
      docs.add(doc);
    }

    @Override
    public void generate(AutoIndent sb) {
      sb.append(cmd);
      for (Bash b : args) {
        sb.append(" ");
        if (b == null) sb.append("NULL");
        else if (b instanceof Other || b instanceof BashFile) b.generate(sb);
        else {
          sb.append("\\\n");
          b.generate(sb);
        }
      }
      for (Heredoc doc : docs) {
        doc.generate(sb);
      }
      if (docs.size() > 0) sb.append("\n");
    }

    public Bash other(Bash other) {
      args.add(other);
      return this;
    }
  }

  public static class Heredoc implements Bash {

    String content;

    String eof;

    public Heredoc(String eof, String content) {
      this.content = content;
      this.eof = eof;
    }

    @Override
    public void generate(AutoIndent sb) {
      sb.appendRaw("\n");
      sb.appendRaw(content);
      if (!content.endsWith("\n")) sb.appendRaw("\n");
      sb.appendRaw(eof);
    }

  }

  /** Several commands */
  public static class CommandSequence implements Bash {

    String delimiter = "\n";

    List<Bash> commands = new ArrayList<>();

    public Command cmd(String cmd) {
      Command c = new Command(cmd);
      this.commands.add(c);
      return c;
    }

    public void comment(String comment) {
      this.commands.add(new Comment(comment));
    }

    @Override
    public void generate(AutoIndent sb) {
      boolean first = true;
      for (Bash b : commands) {
        if (!first) {
        sb.append(delimiter);
        } else {
          first = false;
        }

        if (b == null) sb.append("NULL");
        else if (b instanceof BashFile) {
          sb.append("cat ");
          b.generate(sb);
        }
        else {
          b.generate(sb);
        }
      }
    }

    public void add(Bash element) {
      commands.add(element);
    }

    public void other(String string) {
      commands.add(new Other(string));
    }

  }

  /** Command sequence where commands are connected with pipes */
  public class Pipe extends CommandSequence {
    public Pipe() {
      this.delimiter = " \\\n | ";
    }

    public Pipe(Bash prev) {
      this();
      commands.add(prev);
    }

  }

  /** Represents a file */
  public static class BashFile implements Bash {

    final String path;

    public BashFile(String path) {
      if (path == null) throw new IllegalArgumentException("file cannot be null");
      this.path = path;
    }

    @Override
    public void generate(AutoIndent sb) {
      sb.append(path);
    }

    public String getPath() {
      return path;
    }
  }

  public static class Comment implements Bash {
    String comment;

    public Comment(String info) {
      this.comment = info;
    }

    @Override
    public void generate(AutoIndent sb) {
      //sb.append(" `# " + comment + "` \\\n");
    }
  }

  public static class Wrap implements Bash {
    final String prefix, suffix;

    final Bash content;

    public Wrap(String prefix, Bash content, String suffix) {
      this.prefix = prefix;
      this.suffix = suffix;
      this.content = content;
    }

    @Override
    public void generate(AutoIndent sb) {
      sb.append(prefix);
      content.generate(sb.indent());
      sb.append(suffix);
    }
  }

  /** Anything that does not fit in the above classes */
  public static class Other implements Bash {

    String text;

    public Other(String text) {
      this.text = text;
    }

    @Override
    public void generate(AutoIndent sb) {
      sb.append(text);
    }
  }

  /** Create a pipe based on the current snippet. Convenience method. */
  public default Pipe pipe() {
    if (this instanceof Pipe) return (Pipe) this;
    Pipe p = new Pipe();
    p.add(this);
    return p;
  }

  /** Prepend and append strings to snippet. Convenience method. */
  public default Bash wrap(String prefix, String suffix) {
    return new Wrap(prefix, this, suffix);
  }

  /** Add a comment at current position */
  public default Bash info(PlanNode node, String str) {
    CommandSequence result;
    if (this instanceof CommandSequence) {
      result = (CommandSequence) this;
    } else {
      result = new CommandSequence();
    }
    result.comment(node.operatorString() + " " + str);
    return result;
  }

}
