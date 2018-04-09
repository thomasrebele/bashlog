package bashlogweb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import bashlog.BashlogCompiler;
import common.DatalogTools;
import common.parser.BashRule;
import common.parser.ParseException;
import common.parser.ParserReader;
import common.parser.Program;
import rdf.OntologyConverter;
import rdf.RDFSpecificTuplesSerializer;
import rdf.RDFTupleSerializer;
import rdf.SPARQLConverter;

@WebServlet({ "/api", "/api/datalog", "/api/sparql" })
public class API extends HttpServlet {

  private static final long serialVersionUID = -4871591970306811662L;

  private static Pattern pApi = Pattern.compile("api.*");

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    URL uurl = new URL(req.getRequestURL().toString());

    String bashlog = null;
    if (uurl.getPath().contains("datalog")) {
      BufferedReader reader = req.getReader();
      String datalog = reader.lines().collect(Collectors.joining("\n"));
      bashlog = processDatalogQuery(datalog, req, resp);
      req.setAttribute("datalog", datalog);

    } else if (uurl.getPath().contains("sparql")) {
      BufferedReader reader = req.getReader();
      String data = reader.lines().collect(Collectors.joining("\n"));
      Map<String, List<String>> params = splitQuery(data);

      bashlog = processSparqlQuery(params, req, resp);
      req.setAttribute("owl", params.get("owl"));
      req.setAttribute("sparql", params.get("sparql"));
      req.setAttribute("nTriples", params.get("nTriples"));
    }

    if (bashlog != null) {
      resp.setContentType("text/plain");
      resp.getWriter().write(bashlog);
      resp.getWriter().close();
      return;
    }

    String url = req.getRequestURL().toString();
    url = pApi.matcher(url.toString()).replaceFirst("api");
    req.setAttribute("url", url.replace(".jsp", ""));
    req.getRequestDispatcher("/api.jsp").forward(req, resp);
  }

  protected static String processDatalogQuery(String datalog, HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (datalog != null) {
      Program p;
      String bashlog = null;
      try {
        p = Program.read(new ParserReader(datalog), BashlogCompiler.BASHLOG_PARSER_FEATURES);
        if (p.rules().size() > 0) {
          String query = null;
          if (req.getParameter("query") != null) {
            query = req.getParameter("query");
          }
          if (query == null || query.trim().isEmpty()) {
            query = p.rules().get(p.rules().size() - 1).head.getRelation();
          }
          bashlog = BashlogCompiler.compileQuery(p, query);
        }
      } catch (ParseException | IllegalArgumentException e) {
        bashlog = e.getMessage();
      }

      return bashlog;
    }
    return null;
  }

  protected static String processSparqlQuery(Map<String, List<String>> params, HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {

    String helperPred = "api_tmp_";
    String queryPred = helperPred + "query";
    String factPred = helperPred + "facts";
    String rdfTypeConst = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

    RDFTupleSerializer tupleSerializer = new RDFSpecificTuplesSerializer(Collections.emptyMap());

    Program query;
    String bashlog = null, sparql = null;
    try {
      // convert sparql
      SPARQLConverter sparqlConverter = new SPARQLConverter(tupleSerializer, helperPred);
      sparql = params.get("sparql").stream().findFirst().orElse("ERROR: no sparql query specified!");
      query = sparqlConverter.convert(sparql, queryPred);

    } catch (Exception e) {
      return "Error parsing sparql query \n" + sparql + "\n" +  e.getMessage();
    }
    
    try {
      // convert OWL
      OWLOntologyManager ontologyManager = OWLManager.createOWLOntologyManager();
      String ontologyCode = params.get("owl").stream().findFirst().orElse("ERROR: no owl ontology specified");

      OWLOntology ontology;
      try {
        ontology = ontologyManager.loadOntologyFromOntologyDocument(new ByteArrayInputStream(ontologyCode.getBytes(StandardCharsets.UTF_8)));
      } catch (OWLOntologyCreationException e) {
        return e.toString();
      }
      OntologyConverter converter = new OntologyConverter(tupleSerializer);
      Program ontologyProgram = converter.convert(ontology);

      // combine
      Program result = Program.merge(ontologyProgram, query);
      List<String> inputRelations = result.allRelations().stream()//
          .filter(s -> !s.startsWith(helperPred)).collect(Collectors.toList());
      Program input = DatalogTools.inputRules3(factPred, inputRelations, rdfTypeConst, true);
      result.addRules(input);

      // ntriple filename
      String filename = params.getOrDefault("nTriples", Collections.emptyList()).stream().findFirst().orElse("\"$@\"");
      String inputRule = factPred + "(X,Y,Z) :~ read_ntriples " + filename;
      result.addRule(BashRule.read(new ParserReader(inputRule), BashlogCompiler.BASHLOG_PARSER_FEATURES));

      System.out.println(result);
      BashlogCompiler bc = BashlogCompiler.prepareQuery(result, queryPred);
      bashlog = bc.compile("", " | conv_ntriples", false);
    }
    catch(Exception e) {
      bashlog = e.getMessage();
    }
    return bashlog;
  }

  // adapted from https://stackoverflow.com/a/13592567/1562506
  public Map<String, List<String>> splitQuery(String urlParams) {
    if (urlParams == null || urlParams == "") {
      return Collections.emptyMap();
    }
    return Arrays.stream(urlParams.split("&")).map(this::splitQueryParameter).collect(
        Collectors.groupingBy(SimpleImmutableEntry::getKey, LinkedHashMap::new, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
  }

  public SimpleImmutableEntry<String, String> splitQueryParameter(String it) {
    final int idx = it.indexOf("=");
    final String key = idx > 0 ? it.substring(0, idx) : it;
    final String value = idx > 0 && it.length() > idx + 1 ? decodeURLString(it.substring(idx + 1)) : null;
    return new SimpleImmutableEntry<>(key, value);
  }

  private static String decodeURLString(String url) {
    try {
      return URLDecoder.decode(url, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
