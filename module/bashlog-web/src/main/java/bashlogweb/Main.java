package bashlogweb;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/index.jsp")
public class Main extends HttpServlet {

  private static final long serialVersionUID = -4646304382919974568L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    System.out.println(req.getRequestURI());
    String datalog = req.getParameter("datalog");

    if (datalog != null) {
      String bashlog = API.processQuery(datalog, req, resp);
      
      if(req.getParameter("download") != null) {
        req.getParameterMap().forEach((k,v) -> System.out.println(k + "  " + Arrays.toString(v)));
        resp.setContentType("application/octet-stream");
        resp.setHeader("Content-Disposition", "filename=\"query.sh\"");
        resp.getOutputStream().write(bashlog.getBytes());
        return;
      }
      
      req.setAttribute("bashlog", bashlog);
    } else {
      datalog = "facts(S,P,O) :~ cat ~/facts.tsv\n" + //
          "main(X) :- facts(X, _, \"person\").";
    }
    req.setAttribute("datalog", datalog);
    req.getRequestDispatcher("convert.jsp").forward(req, resp);
  }
}
