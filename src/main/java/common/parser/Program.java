package common.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javatools.filehandlers.FileUtils;

/**
 * Parses datalog
 * 
 * current grammar:
 *   program ::= rule*
 *   rule ::= compound_term :- compound_term+ '.' | compound_term '.'
 *   compound_term ::= atom '(' term* ')'
 *   term ::= variable | atom | constant
 *   variable ::= name_starting_with_uppercase
 *   atom ::= name_starting_with_lowercase
 *   constant ::= number | string_with_apostrophes
 * 
 * @author Thomas Rebele
 */
public class Program implements Parseable {

  public List<Rule> rules = new ArrayList<Rule>();

  public static Program loadFile(String path) throws IOException {
    File f = new File(path);
    String content = FileUtils.getFileContent(f);
    return read(new ParserReader(content));
  }

  public static Program read(ParserReader pr) {
    Program p = new Program();
    Rule r;
    do {
      pr.skipComments();
      r = Rule.read(pr);
      if (r != null) {
        p.rules.add(r);
      }
    } while (r != null);
    return p;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    rules.forEach(r -> {
      sb.append(r.toString());
      sb.append("\n");
    });
    return sb.toString();
  }

  public static void main(String[] args) throws IOException {
    Program p = loadFile("data/rules.y4");
    System.out.println(p.toString());
  }
}
