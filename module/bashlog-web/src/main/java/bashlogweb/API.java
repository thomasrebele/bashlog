package bashlogweb;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import bashlog.BashlogCompiler;
import common.parser.ParseException;
import common.parser.ParserReader;
import common.parser.Program;

@WebServlet("/api")
public class API extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    BufferedReader reader = req.getReader();
    String datalog = reader.lines().collect(Collectors.joining("\n"));

    if (datalog != null) {
      Program p;
      String bashlog = null;
      try {
        p = Program.read(new ParserReader(datalog));
        if (p.rules().size() > 0) {
          String query = p.rules().get(p.rules().size() - 1).head.getRelation();
          if (req.getParameter("query") != null) {
            query = req.getParameter("query");
          }
          bashlog = BashlogCompiler.compileQuery(p, query);
        }
      } catch (ParseException | IllegalArgumentException e) {
        bashlog = e.getMessage();
      }

      if (bashlog != null) {
        resp.setContentType("text/plain");
        resp.getWriter().write(bashlog);
        resp.getWriter().close();
        return;
      }

    }
    req.setAttribute("datalog", datalog);
    req.getRequestDispatcher("api.jsp").forward(req, resp);
  }
}
