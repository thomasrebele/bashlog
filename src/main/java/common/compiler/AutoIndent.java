package common.compiler;

/**
 * StringBuilder replacement which allows indenting of code blocks.
 */
public class AutoIndent {

  private static final String TAB = "    ";

  /** The string built so far */
  private StringBuilder sb;

  /** The current indent */
  private String indent;

  /** Default constructor */
  public AutoIndent() {
    indent = "";
    this.sb = new StringBuilder();
  }

  /** Increase the indent */
  public AutoIndent indent() {
    AutoIndent r = new AutoIndent();
    r.sb = sb;
    r.indent = indent + TAB;
    return r;
  }

  /** Append string, respecting the current indentation level */
  public AutoIndent append(String string) {
    sb.append(string.replaceAll("\n", "\n" + indent));
    return this;
  }

  /** Append string representation of an Object. Convenience method */
  public AutoIndent append(Object o) {
    if (o instanceof AutoIndent) {
      append(o.toString());
    }
    else {
      append(o.toString());
    }
    return this;
  }

  /** The string that we have generated so far */
  public String generate() {
    return sb.toString();
  }

  @Override
  public String toString() {
    return generate();
  }

  /** Append string as it is */
  public void appendRaw(String string) {
    sb.append(string);
  }
}