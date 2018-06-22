<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="t" tagdir="/WEB-INF/tags" %>

<t:page>
     
    <jsp:body>
<div class="nograil">

<p>
This project translates datalog programs to Unix shell scripts. It can be used to preprocess large tabular datasets.
</p>


<p>
<h1>Features</h1>

<ul>
<li>translate a Datalog program to a Bash script (&rarr; <a href="${pageContext.request.contextPath}/datalog">Datalog mode</a>)</li>
<li>translate a SPARQL query and an OWL ontology to a Bash script (&rarr; <a href="${pageContext.request.contextPath}/sparql">SPARQL/OWL mode</a>)</li>
<li>it can be used from the command line (&rarr; <a href="${pageContext.request.contextPath}/api">API</a>)</li>
</ul>
</p>

<p>
<h1>Prerequisites:</h1>
<ul>
	<li>bash (<b>no</b> support for other shells, e.g., sh, tcsh, ksh, zsh)</li>
	<li>POSIX commands cat, join, sort, comm, ... (e.g. from the GNU coreutils package)</li>
	<li>AWK (e.g., MAWK or GNU awk; install MAWK for better performance)</li>
</ul>
</p>

<p>
<h1>Technical information</h1>

<ul>
<li>You can obtain the source code from <a href="https://github.com/thomasrebele/bashlog">GitHub</a>.</li>
<li>We describe how it works in the <a href="https://www.thomasrebele.org/publications/2018_report_bashlog.pdf">technical report</a></li>
</ul>


</p>

</div>
    </jsp:body>
</t:page>