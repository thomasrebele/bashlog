<%@ tag description="Overall Page template" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%-- <%@ attribute name="page" required="false" type="java.lang.String" %> --%>

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
<div>
<a href="${pageContext.request.contextPath}/about.jsp"><span>About</span></a>
<a href="${pageContext.request.contextPath}/datalog"><span>Datalog mode</span></a>
<a href="${pageContext.request.contextPath}/sparql"><span>SPARQL/OWL mode</span></a>
<a href="${pageContext.request.contextPath}/api"><span>API</span></a>
</div>
</div>
</header>

<main>

<jsp:doBody/>
</main>
</body>
</html>
