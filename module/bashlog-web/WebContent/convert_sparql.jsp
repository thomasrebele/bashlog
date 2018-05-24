<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@taglib prefix="t" tagdir="/WEB-INF/tags" %>

<t:page>
    <jsp:body>
    
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
				Enter the path to an N-Triples file into the third textbox.
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
	
		
		<h1>Examples:</h1>
		You can try the examples on this <a href="http://resources.mpi-inf.mpg.de/yago-naga/yago3.1/sample-ntriples.zip">dataset </a> (<a href="https://w3id.org/yago/downloads">source</a>).
		
		<ul class="examples">
		<li> Find people that died in the city where they were born
			<code>BASE &lt;http://yago-knowledge.org/resource/&gt;
SELECT ?X WHERE { 
	?X &lt;wasBornIn&gt; ?Y. 
	?X &lt;diedIn&gt; ?Y. 
}</code>
		</li>

		<li> Living people 
			<code>BASE &lt;http://yago-knowledge.org/resource/&gt;
SELECT ?X WHERE { 
	{ ?X &lt;wasBornIn&gt; []. } 
	UNION 
	{ ?X &lt;wasBornOnDate&gt; []. }

	MINUS
	{ ?X &lt;diedIn&gt; []. } 
	MINUS
	{ ?X &lt;diedOnDate&gt; []. }
}</code>
		</li>

		<li> All people
			<code>BASE &lt;http://yago-knowledge.org/resource/&gt;
PREFIX rdf: &lt;http://www.w3.org/1999/02/22-rdf-syntax-ns#&gt;
SELECT ?X WHERE { 
	?X rdf:type/rdfs:subClassOf* &lt;wordnet_person_100007846&gt;.
}</code>
		</li>

		<li>Facts in the query
			<p>SPARQL
			<code>BASE &lt;http://yago-knowledge.org/resource/&gt;
SELECT ?X WHERE { 
	?X &lt;type&gt; <person>.
}</code>
			</p>

			<p>OWL
			<code>@prefix kb: &lt;http://yago-knowledge.org/resource/&gt; .

kb:albert kb:type kb:person.
kb:marie kb:type kb:person.</code>
			</p>
		</li>
		</ul>
	</div>
</div>
</div>

</div>
</div>

    </jsp:body>
</t:page>