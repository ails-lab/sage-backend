package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.DatasetState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.repository.DatasetRepository;
import edu.ntua.isci.ac.lod.vocabularies.DCTVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

@Service
public class IdentifiersService {

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
    @Qualifier("database")
    private Database database;
    
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
	@Autowired
	private DatasetRepository datasetsRepository;

	@Autowired
	@Qualifier("endpoints-cache")
	private Cache endpointsCache;

	
	//caches should be cleared when datasets are publishsed/unpublished
	
    public class GraphLocation {
    	private TripleStoreConfiguration mainTripleStore;
    	private Resource mainGraph;
    	private boolean publik;
    	
    	private Map<TripleStoreConfiguration, List<Resource>> subGraphMap;

    	public GraphLocation() {
    		subGraphMap = new HashMap<>();
    	}

    	public GraphLocation(Resource mainGraph, TripleStoreConfiguration mainTripleStore) {
    		this.mainGraph = mainGraph;
    		this.mainTripleStore = mainTripleStore;
    		
    		subGraphMap = new HashMap<>();
    	}
    	
    	public void addSubgraph(Resource graph, TripleStoreConfiguration vc) {
    		List<Resource> list = subGraphMap.get(vc);
    		if (list == null) {
    			list = new ArrayList<>();
    			subGraphMap.put(vc, list);
    		}
    		list.add(graph);
    	}

		public TripleStoreConfiguration getMainTripleStore() {
			return mainTripleStore;
		}

		public void setMainTripleStore(TripleStoreConfiguration mainTripleStore) {
			this.mainTripleStore = mainTripleStore;
		}

		public Resource getMainGraph() {
			return mainGraph;
		}

		public void setMainGraph(Resource mainGraph) {
			this.mainGraph = mainGraph;
		}

		public boolean isPublik() {
			return publik;
		}

		public void setPublik(boolean publik) {
			this.publik = publik;
		}

		public Map<TripleStoreConfiguration, List<Resource>> getSubGraphMap() {
			return subGraphMap;
		}

		public void setSubGraphMap(Map<TripleStoreConfiguration, List<Resource>> subGraphMap) {
			this.subGraphMap = subGraphMap;
		}
    }    
    
    public void remove(String identifier) {
    	if (identifier != null) {
    		endpointsCache.remove(identifier);
    	}
    }
    
    public GraphLocation getGraph(String identifier) {
		Element e = endpointsCache.get(identifier);
		if (e != null) {
			return (GraphLocation)e.getObjectValue();
		}
		
    	GraphLocation dl = null;
		
		Optional<Dataset> datasetOpt = datasetsRepository.findByIdentifierAndDatabaseId(identifier, database.getId());
		
		if (!datasetOpt.isPresent()) {
			datasetOpt = datasetsRepository.findByUuid(identifier);
		}
		
		if (!datasetOpt.isPresent()) {
	
			// legacy -- to be removed -- switch to read identifier from mongo 
			
	    	String sparql =  "SELECT ?graph FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { ?graph <" + DCTVocabulary.identifier + "> \"" + identifier + "\" } ";
			
	    	
	    	loop:
	    	for (TripleStoreConfiguration ivc : virtuosoConfigurations.values()) {
		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(ivc.getSparqlEndpoint(), sparql)) {
		//    	try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", sparql)) {
		    	
					ResultSet rs = qe.execSelect();
					
					while (rs.hasNext()) {
						QuerySolution sol = rs.next();
						dl = new GraphLocation(sol.get("graph").asResource(), ivc);
						break loop;
					}
		    	}
		    }
	    	
		} else {
			
			Dataset dataset = datasetOpt.get();
			
 			ProcessStateContainer ps = dataset.getCurrentPublishState(virtuosoConfigurations.values());
 	 		if (ps != null && (((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED 
                   || ((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PRIVATE 
	 				|| ((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PUBLIC)) {

	 	 		dl = new GraphLocation(resourceVocabulary.getDatasetAsResource(dataset.getUuid()), ps.getTripleStoreConfiguration());
	 	 		dl.setPublik(dataset.isPublik());
			} else {
				dl = new GraphLocation(resourceVocabulary.getDatasetAsResource(dataset.getUuid()), null);
				dl.setPublik(dataset.isPublik());
			}
	 	 		
			if (dataset.getDatasets() != null && dataset.getDatasets().size() > 0) {

	 			for (ObjectId subDatasetId : dataset.getDatasets()) {
	 		 		Optional<Dataset> subDatasetOpt = datasetsRepository.findById(subDatasetId);
	 		 		
	 		 		if (!subDatasetOpt.isPresent()) {
	 		 			continue;
	 		 		}
	 		 		
	 		 		Dataset subDataset = subDatasetOpt.get();

	 	 	 		ProcessStateContainer subPs = subDataset.getCurrentPublishState(virtuosoConfigurations.values());
	 	 	 		if (subPs == null || (((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED 
	 	 	 				&& ((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PRIVATE 
	 	 	 				&& ((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PUBLIC)) {
	 	 	 			continue;
	 	 	 		}
	 	 	 		
	 	 	 		if (dl == null) {
	 	 	 			dl = new GraphLocation();
	 	 	 			dl.setPublik(dataset.isPublik());
	 	 	 		}
	 	 	 		
	 				dl.addSubgraph(resourceVocabulary.getDatasetAsResource(subDataset.getUuid()), subPs.getTripleStoreConfiguration());
	 			}
			}
		}

    	endpointsCache.put(new Element(identifier, dl));
    	
    	return dl;
    }	
    
}
