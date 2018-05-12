<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="t" tagdir="/WEB-INF/tags" %>

<t:page>
    <jsp:body>


<div class="grail">
	<div class="grail-body">

<div class="grail-content">

<h1>Sparql/OWL</h1>

An API for transforming SPARQL queries and an OWL ontology to bash scripts. <br>

Please call it as follows:

<code>curl --data-urlencode owl@<i style="color:red;">ontology.owl</i> --data-urlencode sparql@<i style="color:red;">query.sparql</i> --data-urlencode nTriples=<i style="color:red;">/path/to/kb.ntriples</i> ${url}/sparql</code>

where <code class="inline">ontology.owl</code> is a file containing the OWL ontology
and <code class="inline">sparql.query</code> is a file containing the SPARQL query

<br>

You can save the script in a file by changing the command as follows:

<code>curl <i style="color:red;">...</i> &gt; query.sh</code>

Execute it with the command <code class="inline">bash query.sh</code>.

</div>

<div class="grail-content">

<h1>Datalog</h1>

An API for transforming datalog to bash scripts. 
For the syntax of the datalog dialect, see the <a href="./datalog">datalog page</a> <br><br>

Please call it as follows:

<br>

<code>curl --data-binary @<i style="color:red;">example.dlog</i> ${url}/datalog\?query=<i style="color:red;">predicate</i></code>

where <code class="inline">example.dlog</code> contains your datalog program, here an example:

<code>facts(S,P,O) :~ cat ~/facts.tsv
main(X) :- facts(X, _, "person").
</code>

<br>

You can save the script in a file by changing the command as follows:

<code>curl <i style="color:red;">...</i> &gt; query.sh</code>

Execute it with the command <code class="inline">bash query.sh</code>.

</div>

</div>
</div>
    </jsp:body>
</t:page>