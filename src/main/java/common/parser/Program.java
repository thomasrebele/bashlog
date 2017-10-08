package common.parser;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

  private List<Rule> rules = new ArrayList<Rule>();

  private Map<String, List<Rule>> relationToRules = new HashMap<>();

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
        p.addRule(r);
      }
    } while (r != null);
    return p;
  }

  public void addRule(Rule rule) {
    rules.add(rule);
    relationToRules.computeIfAbsent(rule.head.getRelation(), k -> new ArrayList<>()).add(rule);
  }

  public void addRules(Program p) {
    p.rules().forEach(this::addRule);
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

  public List<Rule> rules() {
    return rules;
  }

  public List<Rule> rulesForRelation(String relation) {
    return relationToRules.getOrDefault(relation, Collections.emptyList());
  }

  public Set<String> getDependencies(String relation) {
    Set<String> result = new HashSet<>();
    getDependencies(relation, result, new HashSet<>());
    return result;
  }

  private void getDependencies(String relation, Set<String> result, Set<Rule> doneRules) {
    if (result.contains(relation) || !relationToRules.containsKey(relation)) return;
    for (Rule rule : relationToRules.get(relation)) {
      getDependencies(rule, result, doneRules);
    }
  }

  public Set<String> getDependencies(Rule rule) {
    Set<String> result = new HashSet<>();
    getDependencies(rule, result, new HashSet<>());
    return result;
  }

  public void getDependencies(Rule rule, Set<String> result, Set<Rule> doneRules) {
    if (doneRules.contains(rule)) return;
    doneRules.add(rule);
    for (String relation : rule.getDependencies()) {
      getDependencies(relation, result, doneRules);
      result.add(relation);
    }
  }

  public static Program merge(Program p1, Program p2) {
    Program result = new Program();
    p1.rules().forEach(result::addRule);
    p2.rules().forEach(result::addRule);
    return result;
  }

  public static void main(String[] args) throws IOException {
    Program p = loadFile("data/rules.y4");
    System.out.println(p.toString());
  }

}
