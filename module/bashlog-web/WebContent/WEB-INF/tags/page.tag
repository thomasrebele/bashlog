<%@ tag description="Overall Page template" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>

<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=2.0">
<title>Bashlog API</title>
<!-- <link rel="stylesheet" type="text/css" href="reset.css"> -->
<link rel="stylesheet" type="text/css" href="${pageContext.request.contextPath}//style.css">

</head>
<body style="zoom: 120%;">

<header>
<div class="headline">
<span style="font-weight: bold; font-size: 20px;">Bash Datalog</span><br/>
Answering Datalog Queries with Unix Shell Commands
</div>
<div class="menu">
<a href="${pageContext.request.contextPath}/about.jsp">About</a>
<a href="${pageContext.request.contextPath}/datalog">Datalog mode</a>
<a href="${pageContext.request.contextPath}/sparql">SPARQL/OWL mode</a>
<a href="${pageContext.request.contextPath}/api">API</a>
</div>
</header>

<main>



<jsp:doBody/>
</main>
</body>
</html>
