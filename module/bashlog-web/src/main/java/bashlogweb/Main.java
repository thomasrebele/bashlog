package bashlogweb;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet({ "/index.jsp", "/datalog", "/sparql" })
public class Main extends HttpServlet {

  private static final long serialVersionUID = -4646304382919974568L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    URL uurl = new URL(req.getRequestURL().toString());

    String bashlog = "";
    String page = "about.jsp";
    if (uurl.getPath().contains("datalog")) {
      String datalog = req.getParameter("datalog");

      if (datalog != null) {
        bashlog = API.processDatalogQuery(datalog, req, resp);
      } else {
        datalog = "facts(S,P,O) :~ cat ~/facts.tsv\n" + //
            "main(X) :- facts(X, _, \"person\").";
      }
      req.setAttribute("datalog", datalog);
      page = "convert_datalog.jsp";

    } else if(uurl.getPath().contains("sparql")) {
      String owl = req.getParameter("owl");
      String sparql = req.getParameter("sparql");
      String nTriples = req.getParameter("nTriples");
      
      if(sparql != null) {
        HashMap<String, List<String>> params = new HashMap<>();
        params.put("owl", Collections.singletonList(owl));
        params.put("sparql", Collections.singletonList(sparql));
        params.put("nTriples", Collections.singletonList(nTriples));
        
        bashlog = API.processSparqlQuery(params, req, resp);
      } else {
        sparql = "BASE <http://yago-knowledge.org/resource/>\n" +
            "SELECT ?x WHERE { ?X rdf:type <wordnet_person_100007846> }";
      }
      if(nTriples == null) {
        nTriples = "/path/to/knowledge-base.ntriples";
      }
      
      req.setAttribute("owl", owl);
      req.setAttribute("sparql", sparql);
      req.setAttribute("nTriples", nTriples);
      page = "convert_sparql.jsp";
    }

    req.setAttribute("bashlog", bashlog);

    if (req.getParameter("download") != null) {
      req.getParameterMap().forEach((k, v) -> System.out.println(k + "  " + Arrays.toString(v)));
      resp.setContentType("application/octet-stream");
      resp.setHeader("Content-Disposition", "filename=\"query.sh\"");
      resp.getOutputStream().write(bashlog.getBytes());
      return;
    }

    req.getRequestDispatcher(page).forward(req, resp);
  }
}
