package bashlog.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import bashlog.BashlogCompiler;
import bashlog.BashlogEvaluator;
import common.parser.ParseException;
import common.parser.ParserReader;
import common.parser.Program;

@WebServlet("/")
public class Main extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String datalog = req.getParameter("datalog");
		if (datalog != null) {
			Program p;
			try {
				p = Program.read(new ParserReader(datalog));

				String query = p.rules().get(p.rules().size() - 1).head.getRelation();
				if (req.getParameter("query") != null) {
					query = req.getParameter("query");
				}
				String bashlog = BashlogCompiler.compileQuery(p, query);

				req.setAttribute("bashlog", bashlog);
			} catch (ParseException e) {
				req.setAttribute("bashlog", e.getMessage());
			}
		} else {
			datalog = "facts(S,P,O) :~ cat ~/facts.tsv\n" + //
					"main(X) :- facts(X, _, \"person\").";
		}
		req.setAttribute("datalog", datalog);
		req.getRequestDispatcher("convert.jsp").forward(req, resp);
	}
}
