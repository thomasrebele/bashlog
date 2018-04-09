<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=2.0">
<title>Bashlog</title>

<!-- <link rel="stylesheet" type="text/css" href="reset.css"> -->
<link rel="stylesheet" type="text/css" href="style.css">

</head>
<body style="zoom: 120%;">

<header>
<span style="font-weight: bold; font-size: 20px;">Bash Datalog</span><br/>
Answering Datalog Queries with Unix Shell Commands
</header>


<main>
	<%@include file="links.jsp" %>
<div class="grail">
	<div class="grail-body">

<div class="grail-content">

	<form action="${pageContext.request.contextPath}/sparql" accept-charset="utf-8" method="POST">

	<h1>SPARQL query</h1>
	<textarea name="sparql">${ sparql }</textarea>

	<h1>OWL ontology</h1>
	<textarea name="owl">${ owl }</textarea>

	<h1>N-Triple input file</h1>
	<textarea style="height: 2em;" name="nTriples">${ nTriples }</textarea>

	<br>
	<!-- Main predicate: <input name="query">${query }</input> (default: head of last rule)
	<br>-->

	<center style="width: 95%;">
	<button name="convert">Convert to bash script</button>
	<button name="download">Download script</button>
	</center>

	<br>
	<h1>Bash script</h1>
	<textarea name="bash">${ bashlog }</textarea>
	</form>
	

</div>


<div class="grail-content">
	<div style="border: 0px;">
	
		<h1>How to try it:</h1>
			<ol>
			<li>
				Enter a SPARQL query in the first textbox.
			</li>
			<li>
				Optional: Enter an OWL ontology in the second textbox
			</li>
			<li>
				Enter the path to a N-Triples file into the third textbox.
			</li>
			<li>
				Click on the <code class="inline">Convert to bash script</code> button
			</li>
			<li>
				Copy the content of the "Bash script" textbox into a file named <code class="inline">query.sh</code> in the folder with the .tsv files 
				(or click on <code class="inline">Download script</code>)
			</li>
			<li>
				Run it with <code class="inline">bash query.sh</code>
			</li>
			</ol>
			<b>Note</b>: the script uses a folder <code class="inline">tmp</code> for temporary files and removes its contents afterwards
		<h1>API</h1>
		You can also use bashlog from the command line, without a browser. For details, see <a href="api">API</a>.
	
	
		<h2>Prerequisites:</h2>
		<ul>
			<li>bash (<b>no</b> support for other shells, e.g., sh, tcsh, ksh, zsh)</li>
			<li>POSIX commands cat, join, sort, comm, ... (e.g. from the GNU coreutils package)</li>
			<li>MAWK or GNU awk (choose MAWK for better performance)</li>
		</ul>
		
		<!-- <h1>Examples:</h1> -->
		
	</div>
</div>
</div>

</div>
</div>
</main>
</body>
</html>
