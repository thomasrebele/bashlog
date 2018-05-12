<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="t" tagdir="/WEB-INF/tags" %>

<t:page>
    <jsp:body>
<div class="nograil">

This project translates datalog programs to Unix shell scripts. It can be used to preprocess large tabular datasets.
It has a <a href="${pageContext.request.contextPath}/datalog">datalog</a> mode, 
a <a href="${pageContext.request.contextPath}/sparql">SPARQL/OWL</a> mode, 
and an <a href="${pageContext.request.contextPath}/api">API</a>.
You can obtain the source code <a href="https://www.thomasrebele.org/publications/2018_bashlog.zip">here</a>.
We describe how it works in the <a href="https://www.thomasrebele.org/publications/2018_report_bashlog.pdf">technical report</a>.
</div>
    </jsp:body>
</t:page>