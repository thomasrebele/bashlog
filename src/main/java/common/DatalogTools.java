package common;

import java.util.Collection;

import common.parser.CompoundTerm;
import common.parser.Constant;
import common.parser.Program;
import common.parser.Rule;
import common.parser.Term;
import common.parser.Variable;

public class DatalogTools {

  /** Add input rules for classes and binary relations for predicates occurring in 'p'. 
   * @param modeURI wrap in &lt; ... &gt; if true
   * */
  public static Program inputRules3(String factPred, Program p, String rdfType, boolean modeURI) {
    return inputRules3(factPred, p.allRelations(), rdfType, modeURI);
  }
  
  /** Add input rules for classes and binary relations for predicates specified by 'relations'
  * @param modeURI wrap in &lt; ... &gt; if true
  * */
  public static Program inputRules3(String factPred, Collection<String> relations, String rdfType, boolean modeURI) {
    Program result = new Program();
    
    Variable X = new Variable("X");
    Variable Y = new Variable("Y");
    
    Term rdfTypeConst = Constant.of(rdfType);
    for (String rel : relations) {
      int pos = rel.lastIndexOf("/");
      if(pos < 0) continue;
      String arity = rel.substring(pos+1);
      String relation = rel.substring(0, pos);
      Term relConst = modeURI ? Constant.of(ensureURI(relation)) : Constant.of(relation);
      if ("1".equals(arity)) {
        // abc(X) :- facts(X, "rdf:type", "abc").";
        Rule r = new Rule(
            new CompoundTerm(relation, X), // head
            new CompoundTerm(factPred, X, rdfTypeConst, relConst));
        result.addRule(r);
        
      } else if ("2".equals(arity)) {
        // abc(X,Y) :- facts(X, "abc", Y).
        Rule r = new Rule(
            new CompoundTerm(relation, X, Y), // head
            new CompoundTerm(factPred, X, relConst, Y));
        result.addRule(r);
      }
    }
    return result;
  }

  private static String ensureURI(String str) {
    return (str.startsWith("<") ? "" : "<") + str + (str.endsWith(">") ? "" : ">");
  }
  
}
