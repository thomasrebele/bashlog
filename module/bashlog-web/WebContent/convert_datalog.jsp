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
	
	<form action="${pageContext.request.contextPath}/datalog" accept-charset="utf-8" method="POST">
	<h1>Datalog program</h1>
	<textarea name="datalog">${ datalog }</textarea>

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
	
	<h1>How to try it:</h1>
		<ol>
		<li>
			Copy one of the examples into the "Datalog program" textbox
		</li>
		<!-- <li>
			<span style="font-size: 12px;">(The last rule's head specifies the query. If you want to query a different predicate, you need to enter a predicate into the "Main predicate" textbox
			in the form <i>predicateName/arity</i>, or just <i>predicateName</i>, if there's only one arity.)</span>
		</li>-->
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
	You can also use bashlog from the command line, without a browser. For details, see <a href="${pageContext.request.contextPath}/api">API</a>.
</div>


<div class="grail-content">
	<div style="border: 0px;">
		<h1>Examples:</h1>
		You can try the examples on this <a href="http://resources.mpi-inf.mpg.de/yago-naga/yago3.1/sample.zip">dataset </a> (<a href="https://w3id.org/yago/downloads">source</a>).
		Unpack the dataset archive in a new folder (if unzip is installed: <code class="inline">unzip sample.zip</code>).
		
		<ul>
		<li> Find people that died in the city where they were born

		<code>facts(_, S, P, O) :~ cat *.tsv
main(X) :- 
   facts(_, X, "&lt;wasBornIn&gt;", Y),
   facts(_, X, "&lt;diedIn&gt;", Y).</code>
		<br/>
		</li>
		
		<li> Living people 

		<code>facts(_, S, P, O) :~ cat *.tsv
born(X) :- facts(_, X, "&lt;wasBornIn&gt;", Y).
born(X) :- facts(_, X, "&lt;wasBornOnDate&gt;", Y).
dead(X) :- facts(_, X, "&lt;diedIn&gt;", Y).
dead(X) :- facts(_, X, "&lt;diedOnDate&gt;", Y).

main(X) :- born(X), not dead(X).</code>
			(you can find deceased people by removing <code class="inline">not</code>)
			<br/><br/>
		</li>
		
		<li> All people
		<code>facts(_, S, P, O) :~ cat *.tsv

type(X, Y) :- facts(_, X, "rdf:type", Y).
subclass(X, Y) :- facts(_, X, "rdfs:subclassOf", Y).
type(X, Z) :- type(X, Y), subclass(Y, Z).

main(X) :- type(X, "&lt;wordnet_person_100007846&gt;").</code>
		<br/>
		</li>
		<li>Facts in a datalog program
			<code>type("albert", "person").
type("marie", "person").
people(X) :- type(X, "person").
</code>
		<br/>
		</li>
		
		</ul>
		<h1>Syntax:</h1>

		Fact:
		<code><i>head</i>(<i>Const1</i>, <i>Const2</i>)).</code>
		<br>

		Basic rule:
		<code><i>head</i>(<i>VarConst1</i>, <i>VarConst3</i>) <b>:-</b> <i>rel1</i>(<i>VarConst1</i>, <i>VarConst2</i>), <i>rel2</i>(<i>VarConst2</i>, <i>VarConst3</i>).</code>
		<br>
		with negation:
		<code><i>head</i>(<i>VarConst1</i>, <i>Var2</i>) <b>:-</b> <i>rel1</i>(<i>VarConst1</i>, <i>Var2</i>), <b>not</b> <i>rel2</i>(<i>Var2</i>).</code>
		<br>

		Bash rule, taking input from bash command:
		<code>head(Var1, Var2) <b>:~</b> bash_command --options arg1 arg2 <span style="color:gray;">&lt;newline&gt;</span></code>
		<br>

		<h2>Hints:</h2>
		<ul>
			<li>the head of the last rule specifies the query</li>
			<li>variables must start with an upper case letter</li>
			<li>every head variable has to appear in the body</li>
			<li>predicates are lower case</li>
			<li>you can use Bash argument variables $1, $2, ..., in Bash rules.
			Specify them when calling the script: <code class="inline">bash query.sh <i>file1</i> <i>file2</i></code></li>
		</ul>

		<h2>Prerequisites:</h2>
		<ul>
			<li>bash (<b>no</b> support for other shells, e.g., sh, tcsh, ksh, zsh)</li>
			<li>POSIX commands cat, join, sort, comm, ... (e.g. from the GNU coreutils package)</li>
			<li>AWK (e.g., MAWK or GNU awk; install MAWK for better performance)</li>
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
