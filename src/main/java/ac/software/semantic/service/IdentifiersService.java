package ac.software.semantic.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.AnnotatorDocument;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.ProjectDocument;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.User;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.repository.core.AnnotatorDocumentRepository;
import ac.software.semantic.repository.core.DatasetRepository;
import ac.software.semantic.repository.core.EmbedderDocumentRepository;
import ac.software.semantic.repository.core.ProjectDocumentRepository;
import ac.software.semantic.repository.core.UserRepository;

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
	private DatasetRepository datasetsRepository;

	@Autowired
	private UserRepository userRepository;

	@Autowired
	private ProjectDocumentRepository projectRepository;

	@Lazy
	@Autowired
	private AnnotatorDocumentRepository annotatorRepository;

	
	@Autowired
	@Qualifier("endpoints-cache")
	private Cache endpointsCache;
	
	//caches should be cleared when datasets are publishsed/unpublished
	
//	public boolean isValidIdentifier(String s) {
//		return s.matches("^[0-9a-zA-Z\\-\\~\\._:]+$");
//	}
	
    public class GraphLocation {
    	private TripleStoreConfiguration mainTripleStore;
    	private Dataset mainGraph;
    	private boolean publik;
    	
    	private Map<TripleStoreConfiguration, List<Dataset>> subGraphMap;
//    	private  Map<TripleStoreConfiguration, List<SideSpecificationDocument>> extraGraphMap;

    	public GraphLocation() {
    		subGraphMap = new HashMap<>();
//    		extraGraphMap = new HashMap<>();
    	}

    	public GraphLocation(Dataset mainGraph, TripleStoreConfiguration mainTripleStore) {
    		this.mainGraph = mainGraph;
    		this.mainTripleStore = mainTripleStore;
    		
    		subGraphMap = new HashMap<>();
//    		extraGraphMap = new HashMap<>();
    	}
    	
    	public void addSubGraph(Dataset graph, TripleStoreConfiguration vc) {
    		List<Dataset> list = subGraphMap.get(vc);
    		if (list == null) {
    			list = new ArrayList<>();
    			subGraphMap.put(vc, list);
    		}
    		list.add(graph);
    	}
    	
//    	public void addExtraGraph(SideSpecificationDocument graph, TripleStoreConfiguration vc) {
//    		List<SideSpecificationDocument> list = extraGraphMap.get(vc);
//    		if (list == null) {
//    			list = new ArrayList<>();
//    			extraGraphMap.put(vc, list);
//    		}
//    		list.add(graph);
//    	}

		public TripleStoreConfiguration getMainTripleStore() {
			return mainTripleStore;
		}

		public void setMainTripleStore(TripleStoreConfiguration mainTripleStore) {
			this.mainTripleStore = mainTripleStore;
		}

		public Dataset getMainGraph() {
			return mainGraph;
		}

		public void setMainGraph(Dataset mainGraph) {
			this.mainGraph = mainGraph;
		}

		public boolean isPublik() {
			return publik;
		}

		public void setPublik(boolean publik) {
			this.publik = publik;
		}

		public Map<TripleStoreConfiguration, List<Dataset>> getSubGraphMap() {
			return subGraphMap;
		}
		
//		public Map<TripleStoreConfiguration, List<SideSpecificationDocument>> getExtraGraphMap() {
//			return extraGraphMap;
//		}

		public void setSubGraphMap(Map<TripleStoreConfiguration, List<Dataset>> subGraphMap) {
			this.subGraphMap = subGraphMap;
		}
		
//		public void setExtraGraphMap(Map<TripleStoreConfiguration, List<SideSpecificationDocument>> extraGraphMap) {
//			this.extraGraphMap = extraGraphMap;
//		}

    }    
    
    public GraphLocation getGraph(String datasetIdentifier) {
    	return getGraph(null, null, datasetIdentifier);
    }	

    public GraphLocation getGraph(String pu, String puIdentifier, String datasetIdentifier) {
    	String identifier = (pu != null ? pu : "") + "/" + (puIdentifier != null ? puIdentifier : "") + "/" + datasetIdentifier;   
    			
		Element e = endpointsCache.get(identifier);
		if (e != null) {
			return (GraphLocation)e.getObjectValue();
		}
		
    	Optional<ProjectDocument> projectOpt = null;
    	Optional<Dataset> datasetOpt = null;

    	if (pu == null) {
    		projectOpt = projectRepository.findByDatabaseDefaultAndDatabaseId(true, database.getId());
    		
    	} else if (pu.equals("p")) {
    		projectOpt = projectRepository.findByIdentifierAndDatabaseId(puIdentifier, database.getId());
    		
    		if (!projectOpt.isPresent()) {
    			projectOpt = projectRepository.findByUuid(puIdentifier);
    		}
    	}
    	
    	if (pu == null || pu.equals("p")) {
    		if (projectOpt.isPresent()) {
    			datasetOpt = datasetsRepository.findByIdentifierAndProjectIdAndDatabaseId(datasetIdentifier, projectOpt.get().getId(), database.getId());
    		
    			if (!datasetOpt.isPresent()) {
        			datasetOpt = datasetsRepository.findByUuid(datasetIdentifier);
        		}
    			
    		} else {
    			
    			datasetOpt = datasetsRepository.findByIdentifierAndDatabaseId(datasetIdentifier, database.getId());
    			
    			if (!datasetOpt.isPresent()) {
    				datasetOpt = datasetsRepository.findByUuid(datasetIdentifier);
    			}
    		}
    		
    	} else if (pu.equals("u")) {
    		Optional<User> userOpt = userRepository.findByIdentifierAndDatabaseId(puIdentifier, database.getId());

			if (!userOpt.isPresent()) {
				userOpt = userRepository.findByUuid(puIdentifier);
			}
			
			if (!userOpt.isPresent()) {
				return null;
			}
			
			datasetOpt = datasetsRepository.findByIdentifierAndUserIdAndDatabaseId(datasetIdentifier, userOpt.get().getId(), database.getId());
			
			if (!datasetOpt.isPresent()) {
				datasetOpt = datasetsRepository.findByUuid(datasetIdentifier);
			}
		}
    	
		if (!datasetOpt.isPresent()) {
			return null;
		}

		GraphLocation dl = buildDataset(datasetOpt.get());

    	endpointsCache.put(new Element(identifier, dl));
    	
    	return dl;
    }	
    
//    public GraphLocation getGraph(ObjectId datasetId) {
//		Optional<Dataset> datasetOpt = datasetsRepository.findById(datasetId);
//    	
//		if (!datasetOpt.isPresent()) {
//			return null;
//		}
//
//		GraphLocation dl = buildDataset(datasetOpt.get());
//
////    	endpointsCache.put(new Element(identifier, dl));
//    	
//    	return dl;
//    }	    
    
    private GraphLocation buildDataset (Dataset dataset) {
    	GraphLocation dl = null;
		
		ProcessStateContainer<DatasetPublishState> ps = dataset.getCurrentPublishState(virtuosoConfigurations.values());

		if (ps != null && DatasetState.isPublishedState(ps.getProcessState().getPublishState())) {
 	 		dl = new GraphLocation(dataset, ps.getTripleStoreConfiguration());
 	 		dl.setPublik(dataset.isPublik());
		} else {
			dl = new GraphLocation(dataset, null);
			dl.setPublik(dataset.isPublik());
		}
 	 		
		if (dataset.getDatasets() != null && dataset.getDatasets().size() > 0) {

 			for (ObjectId subDatasetId : dataset.getDatasets()) {
 		 		Optional<Dataset> subDatasetOpt = datasetsRepository.findById(subDatasetId);
 		 		
 		 		if (!subDatasetOpt.isPresent()) {
 		 			continue;
 		 		}
 		 		
 		 		Dataset subDataset = subDatasetOpt.get();

 	 	 		ProcessStateContainer<DatasetPublishState> subPs = subDataset.getCurrentPublishState(virtuosoConfigurations.values());
 	 	 		
 	 	 		if (subPs == null || !DatasetState.isPublishedState(subPs.getProcessState().getPublishState())) {
 	 	 			continue;
 	 	 		}
 	 	 		
// 	 	 		if (dl == null) {
// 	 	 			dl = new GraphLocation();
// 	 	 			dl.setPublik(dataset.isPublik());
// 	 	 		}
 	 	 		
 				dl.addSubGraph(subDataset, subPs.getTripleStoreConfiguration());
 			}
		}
		
		return dl;

    }
    
    public List<SideSpecificationDocument> getExtraGraphs(List<Dataset> datasets, String identifiers) {
    	
    	List<SideSpecificationDocument> res = new ArrayList<>();
    	if (identifiers != null) {
    		for (String identifier : identifiers.split("~")) {
		    	Optional<AnnotatorDocument> adocOpt = annotatorRepository.findByUuid(identifier);
		    	if (adocOpt.isPresent()) {
		    		res.add(adocOpt.get());
		    	} else {
		    		for (Dataset dataset : datasets) {
		        		adocOpt = annotatorRepository.findByDatasetIdAndIdentifier(dataset.getId(), identifier);
		            	if (adocOpt.isPresent()) {
		            		res.add(adocOpt.get());
		            	} else {
		            		for (AnnotatorDocument adoc : annotatorRepository.findByDatasetIdAndTags(dataset.getId(), identifier)) {
		            			res.add(adoc);
		            		}
		            	}
		    		}
		    	}
    		}
		    		
    	}
    	
    	return res;
    }

	public String getPreferedIdentifier(Dataset dataset) {
   		if (dataset.getProjectId() != null) {
   			for (ObjectId projectId : dataset.getProjectId()) {
   				Optional<ProjectDocument> projectOpt = projectRepository.findById(projectId);
   			
   				if (projectOpt.isPresent()) {
   					ProjectDocument project = projectOpt.get();
   				
   					if (dataset.getIdentifier() != null) {
   	   					
   	   					if (project.getIdentifier() != null) {
   	   						return "p/" + project.getIdentifier() + "/" + dataset.getIdentifier();
   	   					}
   					}
   				}
   			}
   		}
   		
   		return dataset.getUuid();
	}

    public void remove(Dataset dataset) {
   		endpointsCache.remove("//" + dataset.getUuid());
   		
   		if (dataset.getIdentifier() != null) {
   			endpointsCache.remove("//" + dataset.getIdentifier());
   		}
   		
   		if (dataset.getProjectId() != null) {
   			for (ObjectId projectId : dataset.getProjectId()) {
   				Optional<ProjectDocument> projectOpt = projectRepository.findById(projectId);
   			
   				if (projectOpt.isPresent()) {
   					ProjectDocument project = projectOpt.get();
   				
   					endpointsCache.remove("p/" + project.getUuid() + "/" + dataset.getUuid());
   					
   					if (project.getIdentifier() != null) {
   						endpointsCache.remove("p/" + project.getIdentifier() + "/" + dataset.getUuid());
   					}
   					
   					if (dataset.getIdentifier() != null) {
   						endpointsCache.remove("p/" + project.getUuid() + "/" + dataset.getIdentifier());
   	   					
   	   					if (project.getIdentifier() != null) {
   	   						endpointsCache.remove("p/" + project.getIdentifier() + "/" + dataset.getIdentifier());
   	   					}
   					}
   				}
   			}
   		}
   		
   		Optional<User> userOpt = userRepository.findById(dataset.getUserId());
   		
		if (userOpt.isPresent()) {
			User user = userOpt.get();
		
			endpointsCache.remove("u/" + user.getUuid() + "/" + dataset.getUuid());
			
			if (user.getIdentifier() != null) {
				endpointsCache.remove("u/" + user.getIdentifier() + "/" + dataset.getUuid());
			}
			
			if (dataset.getIdentifier() != null) {
				endpointsCache.remove("u/" + user.getUuid() + "/" + dataset.getIdentifier());
				
				if (user.getIdentifier() != null) {
					endpointsCache.remove("u/" + user.getIdentifier() + "/" + dataset.getIdentifier());
				}
			}
		}
    }    
    
//    public GraphLocation getGraph(String identifier) {
//		Element e = endpointsCache.get(identifier);
//		if (e != null) {
//			return (GraphLocation)e.getObjectValue();
//		}
//		
//    	GraphLocation dl = null;
//		
//		Optional<Dataset> datasetOpt = datasetsRepository.findByIdentifierAndDatabaseId(identifier, database.getId());
//		
//		if (!datasetOpt.isPresent()) {
//			datasetOpt = datasetsRepository.findByUuid(identifier);
//		}
//		
//		if (!datasetOpt.isPresent()) {
//	
//			// legacy -- to be removed -- switch to read identifier from mongo 
//			
//	    	String sparql =  "SELECT ?graph FROM <" + resourceVocabulary.getContentGraphResource() + "> WHERE { ?graph <" + DCTVocabulary.identifier + "> \"" + identifier + "\" } ";
//			
//	    	
//	    	loop:
//	    	for (TripleStoreConfiguration ivc : virtuosoConfigurations.values()) {
//		    	try (QueryExecution qe = QueryExecutionFactory.sparqlService(ivc.getSparqlEndpoint(), sparql)) {
//		//    	try (QueryExecution qe = QueryExecutionFactory.sparqlService("http://192.168.118.1:7200/repositories/stirdata", sparql)) {
//		    	
//					ResultSet rs = qe.execSelect();
//					
//					while (rs.hasNext()) {
//						QuerySolution sol = rs.next();
//						dl = new GraphLocation(sol.get("graph").asResource(), ivc);
//						break loop;
//					}
//		    	}
//		    }
//	    	
//		} else {
//			
//			Dataset dataset = datasetOpt.get();
//			
// 			ProcessStateContainer ps = dataset.getCurrentPublishState(virtuosoConfigurations.values());
// 	 		if (ps != null && (((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED 
//                   || ((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PRIVATE 
//	 				|| ((PublishState)ps.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PUBLIC)) {
//
//	 	 		dl = new GraphLocation(resourceVocabulary.getDatasetAsResource(dataset.getUuid()), ps.getTripleStoreConfiguration());
//	 	 		dl.setPublik(dataset.isPublik());
//			} else {
//				dl = new GraphLocation(resourceVocabulary.getDatasetAsResource(dataset.getUuid()), null);
//				dl.setPublik(dataset.isPublik());
//			}
//	 	 		
//			if (dataset.getDatasets() != null && dataset.getDatasets().size() > 0) {
//
//	 			for (ObjectId subDatasetId : dataset.getDatasets()) {
//	 		 		Optional<Dataset> subDatasetOpt = datasetsRepository.findById(subDatasetId);
//	 		 		
//	 		 		if (!subDatasetOpt.isPresent()) {
//	 		 			continue;
//	 		 		}
//	 		 		
//	 		 		Dataset subDataset = subDatasetOpt.get();
//
//	 	 	 		ProcessStateContainer subPs = subDataset.getCurrentPublishState(virtuosoConfigurations.values());
//	 	 	 		if (subPs == null || (((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED 
//	 	 	 				&& ((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PRIVATE 
//	 	 	 				&& ((PublishState)subPs.getProcessState()).getPublishState() != DatasetState.PUBLISHED_PUBLIC)) {
//	 	 	 			continue;
//	 	 	 		}
//	 	 	 		
//	 	 	 		if (dl == null) {
//	 	 	 			dl = new GraphLocation();
//	 	 	 			dl.setPublik(dataset.isPublik());
//	 	 	 		}
//	 	 	 		
//	 				dl.addSubgraph(resourceVocabulary.getDatasetAsResource(subDataset.getUuid()), subPs.getTripleStoreConfiguration());
//	 			}
//			}
//		}
//
//    	endpointsCache.put(new Element(identifier, dl));
//    	
//    	return dl;
//    }	
    
}
