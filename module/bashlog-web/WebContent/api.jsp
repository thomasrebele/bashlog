<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>BashlogWeb API</title>
<!-- <link rel="stylesheet" type="text/css" href="reset.css"> -->
<link rel="stylesheet" type="text/css" href="style.css">
</head>
<body>

<header>
Bash Datalog<br/>
Answering Datalog Queries with Unix Shell Commands
</header>
<main>

A simple API for obtaining bashlog scripts. Please call it as follows:

<br>

<code>curl --data-binary @example.dlog <%=request.getRequestURL() %>\?query=<i>predicate</i></code>

Where <code class="inline">example.dlog</code> contains your datalog program, here an example:

<code>facts(S,P,O) :~ cat ~/facts.tsv
main(X) :- facts(X, _, "person").
</code>

For the syntax of the datalog dialect see the <a href="./">main page</a>

</main>
</body>
</html>
