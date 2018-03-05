<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=2.0">
<title>Bashlog API</title>
<!-- <link rel="stylesheet" type="text/css" href="reset.css"> -->
<link rel="stylesheet" type="text/css" href="style.css">

</head>
<body style="zoom: 120%;">

<header>
<span style="font-weight: bold; font-size: 20px;">Bash Datalog</span><br/>
Answering Datalog Queries with Unix Shell Commands
</header>


<main class="grail">
	<div class="grail-body">

<div class="grail-content">

A simple API for transforming datalog to bash scripts. 
For the syntax of the datalog dialect, see the <a href="./">main page</a> <br><br>

Please call it as follows:

<br>

<code>curl --data-binary @example.dlog ${url}\?query=<i>predicate</i></code>

where <code class="inline">example.dlog</code> contains your datalog program, here an example:

<code>facts(S,P,O) :~ cat ~/facts.tsv
main(X) :- facts(X, _, "person").
</code>

<br>

You can save it into a file with this command:

<code>curl --data-binary @example.dlog ${url}\?query=<i>predicate</i> &gt; query.sh</code>

Execute it with the command <code class="inline">bash query.sh</code>.

</div>

</div>
</main>
</body>
</html>
