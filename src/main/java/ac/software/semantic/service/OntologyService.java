package ac.software.semantic.service;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.TurtleDocumentFormatFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class OntologyService {

    private final static Logger logger = LoggerFactory.getLogger(OntologyService.class);

    @Autowired
    @Qualifier("query-ontology")
    private OWLOntology queryOntology;
    
    public String getProperties() {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		
		try {
			OWLManager.createOWLOntologyManager().saveOntology(queryOntology,  new TurtleDocumentFormatFactory().createFormat(), os);
		} catch (OWLOntologyStorageException e) {
			e.printStackTrace();
		}

		String ttl = os.toString();
		
		Model model = ModelFactory.createDefaultModel();

		RDFDataMgr.read(model, new StringReader(ttl), null, Lang.TURTLE);
		
		String sparql = 
		"CONSTRUCT { " +
        " ?prop a <http://www.w3.org/2002/07/owl#ObjectProperty> . " +
	    " ?prop <http://www.w3.org/2000/01/rdf-schema#label> ?label }  " +
		"WHERE { " +
        " ?prop a <http://www.w3.org/2002/07/owl#ObjectProperty> . " +
		" OPTIONAL { ?prop <http://www.w3.org/2000/01/rdf-schema#label> ?label } } ";
				
		Writer sw = new StringWriter();

		try (QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(sparql), model)) {
			Model pmodel = qe.execConstruct();
			
			RDFDataMgr.write(sw, pmodel, RDFFormat.JSONLD_EXPAND_PRETTY) ;
		}
		
//		System.out.println(sw);

        return sw.toString();
    }
    

}
