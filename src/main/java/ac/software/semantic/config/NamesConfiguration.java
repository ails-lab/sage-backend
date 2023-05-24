package ac.software.semantic.config;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.service.NamesService;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.common.utils.SimpleTrie;
import edu.ntua.isci.ac.semaspace.query.URIDescriptor;

@Configuration
public class NamesConfiguration {

	@Value("${app.schema.legacy-uris}")
	private boolean legacyUris;

	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private NamesService namesService;
	
	@Bean(name = "vocabularies")
	@DependsOn({ "triplestore-configurations" })
	public VocabulariesBean vocs(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vcs) {
		VocabulariesBean vb = new VocabulariesBean();
		for (TripleStoreConfiguration vc : vcs.values()) {
			vb.setMap(namesService.createVocabulariesMap(vc, legacyUris));
		}

		return vb;
	}
	
	@Bean(name = "all-datasets")
	@DependsOn({ "triplestore-configurations" })
	public VocabulariesMap dataset(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vcs) {
		VocabulariesMap vm = new VocabulariesMap();
		for (TripleStoreConfiguration vc : vcs.values()) {
			namesService.createDatasetsMap(vm, vc, legacyUris);
		}

		return vm;
	}	
	

	@Bean(name = "prefixes")
	@DependsOn({ "triplestore-configurations", "database" })
    public SimpleTrie<URIDescriptor> vocabularyPrefixes(@Qualifier("triplestore-configurations") ConfigurationContainer<TripleStoreConfiguration> vcs) {
		SimpleTrie<URIDescriptor> res = new SimpleTrie<>();
    
		String sparql = legacyUris ?
				"SELECT ?prefix ?type FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " +
				   "?url  a ?type . VALUES ?type { <" + SEMAVocabulary.AssertionCollection + "> <" + SEMAVocabulary.VocabularyCollection + "> } . " +
		           "?url <http://sw.islab.ntua.gr/apollonis/ms/class> " + 
		           "     [ a       <http://sw.islab.ntua.gr/apollonis/ms/VocabularyTerm> ;" + 
		           "       <http://sw.islab.ntua.gr/apollonis/ms/prefix> ?prefix ]  }"
		        :
	        	"SELECT ?prefix ?type FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { " +
				   "?url  a ?type . VALUES ?type { <" + SEMAVocabulary.AssertionCollection + "> <" + SEMAVocabulary.VocabularyCollection + "> } . " +
		           "?url <" + SEMAVocabulary.clazz + "> " + 
		           "     [ a       <" + SEMAVocabulary.VocabularyTerm + "> ;" + 
		           "       <" + SEMAVocabulary.prefix + "> ?prefix ]  }" ;
		
//		System.out.println(QueryFactory.create(sparql.toString(), Syntax.syntaxARQ));
		for (TripleStoreConfiguration vc : vcs.values()) { 
	
			try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(sparql.toString(), Syntax.syntaxARQ))) {
				ResultSet rs = qe.execSelect();
				while (rs.hasNext()) {
					QuerySolution sol = rs.next();
					String prefix = sol.get("prefix").toString();
					String type = sol.get("type").toString();
					res.put(prefix, new URIDescriptor(prefix, type));
				}
			}
		}
		
		return res;

    }
}
