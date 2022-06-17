package ac.software.semantic.service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import ac.software.semantic.model.*;
import ac.software.semantic.payload.MappingInstanceResponse;
import ac.software.semantic.payload.MappingResponse;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import ac.software.semantic.payload.DatasetResponse;
import ac.software.semantic.repository.MappingRepository;
import ac.software.semantic.repository.DatasetRepository;
import ac.software.semantic.repository.FileRepository;
import ac.software.semantic.security.UserPrincipal;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaSource;
import edu.ntua.isci.ac.common.db.IteratorSet;
import edu.ntua.isci.ac.common.db.rdf.RDFJenaConnection;
import edu.ntua.isci.ac.lod.vocabularies.sema.SEMAVocabulary;
import edu.ntua.isci.ac.semaspace.index.Indexer;


@Service
public class DatasetService {

	Logger logger = LoggerFactory.getLogger(DatasetService.class);
	
	@Autowired
	private ModelMapper modelMapper;
	
	@Autowired
	VirtuosoJDBC virtuosoJDBC;
	
	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	private MappingRepository mappingRepository;

	@Autowired
	private MappingsService mappingsService;

	@Autowired
	FileRepository fileRepository;

    @Autowired
    @Qualifier("database")
    private Database database;

    @Autowired
    @Qualifier("virtuoso-configuration")
    private Map<String,VirtuosoConfiguration> virtuosoConfiguration;

    @Autowired
    @Qualifier("elastic-configuration")
    private ElasticConfiguration elasticConfiguration;

    @Autowired
    @Qualifier("filesystem-configuration")
    private FileSystemConfiguration fileSystemConfiguration;

    @Value("${app.schema.legacy-uris}")
    private boolean legacyUris;
    
	public Dataset createDataset(UserPrincipal currentUser, String name, String type, String typeUri, String asProperty, List<ResourceOption> links, ImportType importType) {
		
		String uuid = UUID.randomUUID().toString();
		List<String> typeUris = new ArrayList<>();
		if (typeUri != null) {
			typeUris.add(typeUri);
			
			for (Resource r : SEMAVocabulary.getSuperCollections(typeUri)) {
				String tr = r.toString();
				if (!typeUris.contains(tr)) {
					typeUris.add(tr);
				}
			}
		}
//		typeUris.add(classUri);
		
		Dataset ds = new Dataset();
		ds.setUserId(new ObjectId(currentUser.getId()));
		ds.setName(name);
		ds.setUuid(uuid);
		ds.setType(type);
		ds.setTypeUri(typeUris);
//		ds.setSourceUri(sourceUri);
//		ds.setTargetUri(targetUri);
		ds.setDatabaseId(database.getId());
		ds.setAsProperty(asProperty);

		ds.setImportType(importType);

		if (links != null) {
			ds.setLinks(links);
		}
		
		return datasetRepository.save(ds);
	}
	
	public Dataset updateDataset(UserPrincipal currentUser, ObjectId id, String name, String type, String typeUri, String asProperty, List<ResourceOption> links, ApplicationEventPublisher applicationEventPublisher) {
		
		Optional<Dataset> dsOpt = datasetRepository.findByIdAndUserId(id, new ObjectId(currentUser.getId()));
		
		if (!dsOpt.isPresent()) {
			return null;
		}
		
		List<String> typeUris = new ArrayList<>();
		if (typeUri != null) {
			typeUris.add(typeUri);
			
			for (Resource r : SEMAVocabulary.getSuperCollections(typeUri)) {
				String tr = r.toString();
				if (!typeUris.contains(tr)) {
					typeUris.add(tr);
				}
			}
		}
//		typeUris.add(classUri);
		
		Dataset ds = dsOpt.get();
		
		ds.setName(name);
		ds.setType(type);
		ds.setTypeUri(typeUris);
		ds.setAsProperty(asProperty);
		
		if (links != null) {
			ds.setLinks(links);
		} else {
			ds.setLinks(new ArrayList<>());
		}

		// This is Europeana Import specific part, should not be executed elsewhere.
		if (ds.getImportType() == ImportType.EUROPEANA) {
			List<MappingDocument> headerMappings = mappingsService.getHeaderMappings(currentUser, id);
			if (headerMappings.size() == 1) {
				MappingDocument doc = headerMappings.get(0);
				List<MappingInstance> instanceList =doc.getInstances();
				if (instanceList.size() == 1) {
					MappingInstance inst = instanceList.get(0);
					inst.setBinding(Arrays.asList(new ParameterBinding("COLLECTION", name)));
					mappingRepository.save(doc);
					mappingsService.executeMapping(currentUser, doc.getId().toString(), inst.getId().toString(), applicationEventPublisher);
					return datasetRepository.save(ds);
				}
			} else {
				return datasetRepository.save(ds);
			}
		}
		return datasetRepository.save(ds);
	}
	
	public boolean deleteDataset(UserPrincipal currentUser, String id) {

		//TODO: should delete catalog members
		List<MappingDocument> datasetMappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		for (MappingDocument map : datasetMappings) {
			mappingsService.deleteMapping(currentUser, map.getId().toString());
		}

		Long r = datasetRepository.deleteByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		if (r > 0) {
			return true;
		} else {
			return false;
		}
		
//		String sparql =
//	    "ASK WHERE { " +
//	         "GRAPH <" + SEMAVocabulary.accessGraph + "> { " +                        
//	            "<" + SEMAVocabulary.getGroup(currentUser.getVirtuosoId()) + "> <" + SACCVocabulary.dataset + "> <" + uri + "> } }";
//		
//		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuoso.endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ));
//		boolean ok = qe.execAsk();
//
//		if (ok) {
//			virtuoso.nestedDelete(SEMAVocabulary.contentGraph.toString(), uri);
//
//			sparql = 
//			"WITH <" + SEMAVocabulary.accessGraph + "> " +
//			"DELETE { ?group <" + SACCVocabulary.dataset + "> <" + uri + "> } " +
//			"WHERE { ?group <" + SACCVocabulary.dataset + "> <" + uri + "> } ";
//
//			VirtGraph vgraph = virtuoso.getVirtGraph();
//			VirtuosoUpdateFactory.create(sparql, vgraph).exec();
//			vgraph.close();
//
//		}
//		
//		return ok;
	}
	
	public List<DatasetResponse> getDatasets(UserPrincipal currentUser, String type) {
		
		List<Dataset> datasets = datasetRepository.findByUserIdAndTypeAndDatabaseId(new ObjectId(currentUser.getId()), type, database.getId());

        List<DatasetResponse> response = datasets.stream()
        		.map(doc -> modelMapper.dataset2DatasetResponse(virtuosoConfiguration.values(), doc))
        		.collect(Collectors.toList());
        
//        System.out.println(response);
        return response;
		
//		String sparql =
//        "CONSTRUCT { ?catalog <http://www.w3.org/2000/01/rdf-schema#label> ?name }" +
//          " WHERE { " +
//             "GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//                "?catalog a <http://www.w3.org/ns/dcat#Dataset> ; " +
//                    "<http://www.w3.org/2000/01/rdf-schema#label> ?name . } " +
//             "GRAPH <" + SEMAVocabulary.accessGraph + "> { " +                        
//                "?group <" + SACCVocabulary.dataset + "> ?catalog ; " +
//                    "<" + SACCVocabulary.member + "> <" +  SEMAVocabulary.getUser(currentUser.getVirtuosoId()) + "> . } }";
//
//		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuoso.endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ));
//		Model model = qe.execConstruct();
//		
//		Writer sw = new StringWriter();
//		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//
////		System.out.println(sw);
//
//        return sw.toString();
		
		
	}
	
	public Optional<DatasetResponse> getDataset(UserPrincipal currentUser, String id) {
		
		Optional<Dataset> dataset = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (dataset.isPresent()) {
			return Optional.of(modelMapper.dataset2DatasetResponse(virtuosoConfiguration.values(), dataset.get()));
		} else {
			return Optional.empty();
		}
        
//		String sparql =
//        "CONSTRUCT { <" + uri + "> <http://www.w3.org/2000/01/rdf-schema#label> ?name }" +
//          " WHERE { " +
//             "GRAPH <" + SEMAVocabulary.contentGraph + "> { " +
//                "<" + uri + "> a <http://www.w3.org/ns/dcat#Dataset> ; " +
//                    "<http://www.w3.org/2000/01/rdf-schema#label> ?name . } " +
//             "GRAPH <" + SEMAVocabulary.accessGraph + "> { " +                        
//                "<" + SEMAVocabulary.getGroup(currentUser.getVirtuosoId()) + "> <" + SACCVocabulary.dataset + "> <" + uri + "> ; " +
//                    "<" + SACCVocabulary.member + "> <" +  SEMAVocabulary.getUser(currentUser.getVirtuosoId()) + "> . } }";
//
//		QueryExecution qe = QueryExecutionFactory.sparqlService(virtuoso.endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ));
//		Model model = qe.execConstruct();
//		
//		Writer sw = new StringWriter();
//		RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//
//        return sw.toString();
	}
	
	public boolean insert(UserPrincipal currentUser, String id, String toId) {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(toId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		dataset.addDataset(new ObjectId(id));
		
		datasetRepository.save(dataset);
		
		return true;
	}
	
	public boolean remove(UserPrincipal currentUser, String id, String fromId) {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(fromId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		
		dataset.removeDataset(new ObjectId(id));
		
		datasetRepository.save(dataset);
		
		return true;
	}
	

	public boolean indexDataset(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();

		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}
		
		IndexState is = dataset.getIndexState(elasticConfiguration.getId());
		
		if (dataset.getType().startsWith("vocabulary-dataset")) {
			
			is.setIndexState(IndexingState.INDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint());
			ic.indexVocabulary(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexVocabularyName(), SEMAVocabulary.getDataset(dataset.getUuid()).toString(), legacyUris);
			
			logger.info("Indexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
		} else if (dataset.getType().startsWith("collection-dataset")) {
            
            is.setIndexState(IndexingState.INDEXING);
            is.setIndexStartedAt(new Date(System.currentTimeMillis()));
            datasetRepository.save(dataset);

            Indexer ic = new Indexer(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint());
            ic.indexCollection(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexDataName(), SEMAVocabulary.getDataset(dataset.getUuid()).toString(), null);
            
            logger.info("Indexing of " + dataset.getId() + " completed");
            
            is.setIndexState(IndexingState.INDEXED);
            is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
            datasetRepository.save(dataset);
            
            return true;
            
        }
		
		return false;
	}
	
	public boolean unindexDataset(UserPrincipal currentUser, String datasetId) throws IOException {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(datasetId), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		
		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}
	
		IndexState is = dataset.getIndexState(elasticConfiguration.getId());
		
		if (dataset.getType().startsWith("vocabulary-dataset")) {
			
			is.setIndexState(IndexingState.UNINDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint());
			ic.unindexVocabulary(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexVocabularyName(), SEMAVocabulary.getDataset(dataset.getUuid()).toString(), legacyUris);
			
			logger.info("Unindexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.NOT_INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
			
		} else if (dataset.getType().startsWith("collection-dataset")) {
			
			is.setIndexState(IndexingState.UNINDEXING);
			is.setIndexStartedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);

			Indexer ic = new Indexer(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint());
			ic.unindexCollection(elasticConfiguration.getIndexIp(), elasticConfiguration.getIndexDataName(), SEMAVocabulary.getDataset(dataset.getUuid()).toString());
			
			logger.info("Unindexing of " + dataset.getId() + " completed");
			
			is.setIndexState(IndexingState.NOT_INDEXED);
			is.setIndexCompletedAt(new Date(System.currentTimeMillis()));
			datasetRepository.save(dataset);
			
			return true;
			
		}
		
		return false;
	}
	

	public DatasetMessage checkPublishVocabulary(UserPrincipal currentUser, String virtuoso, String id) throws Exception {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return DatasetMessage.DATASET_DOES_NOT_EXIST;
		}
		
		Dataset dataset = doc.get();
		
		if (!dataset.getType().equals("vocabulary-dataset")) {
			return DatasetMessage.OK;
		}
		
		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		List<FileDocument> files = fileRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		String mf = getMappingsFolder(currentUser);
    	
		int count = 0;
		String identifier = null;
		
    	try (RDFJenaSource source  = new RDFJenaSource()) {
    	
			for (MappingDocument map : mappings) {
	    		boolean hi = !map.getParameters().isEmpty();
	
				for (MappingInstance mi : map.getInstances()) {
	    			ExecuteState es = mi.checkExecuteState(fileSystemConfiguration.getId());
	
					for (int i = 0; i < Math.max(1, es.getExecuteShards()); i++) {
						String fileName = map.getUuid() + (hi ? "_" + mi.getId().toString() : "") + (i == 0 ? "" : "_#" + i) + ".trig";
					
		    			if (dataset.getType().equals("annotation-dataset")) {
						} else {
	    	    			
			    			if (map.getType() == MappingType.HEADER) {
			    				System.out.println("LOADING " + mf + fileName);
			    				((RDFJenaConnection)source.getConnection()).load(new File(mf + File.separator + fileName), SEMAVocabulary.contentGraph, Lang.TTL);
					    	}
						}
	    			}
				}
			}
			
	//		String labelSparql =  "SELECT ?label GRAPH <" + SEMAVocabulary.contentGraph + "> " + 
	//		                 "WHERE { <" + SEMAVocabulary.getDataset(id) + "> <" + RDFSVocabulary.label + "> ?label";
	
			String getIdentifier = "SELECT ?identifier FROM <" + SEMAVocabulary.contentGraph + "> " + 
								 "WHERE { <" + SEMAVocabulary.getDataset(dataset.getUuid()) + "> <http://purl.org/dc/elements/1.1/identifier> ?identifier . }";
			
			try (IteratorSet iter = source.getConnection().executeQuery(getIdentifier)) {
				while (iter.hasNext()) {
					System.out.println(count);
					count++;
					identifier = iter.next().get(0).toString();
				}
			}
    	}		
    	
		if (count == 0) {
			return DatasetMessage.NO_IDENTIFIER;
		} else if (count > 1) {
			return DatasetMessage.MULTIPLE_IDENTIFIERS;
		}
		
		String checkIdentifier = "ASK FROM <" + SEMAVocabulary.contentGraph + "> " + 
				 "WHERE { ?p a <http://www.w3.org/ns/dcat#Dataset> . " + 
				        " ?p <http://purl.org/dc/elements/1.1/identifier> <" + identifier + "> . }";

		try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.get(virtuoso).getSparqlEndpoint(), QueryFactory.create(checkIdentifier))) {
			
			if (qe.execAsk()) {
				return DatasetMessage.IDENTIFIER_ALREADY_EXISTS;
			}
		}
		
		return DatasetMessage.OK;

		
//		String uf = getUploadsFolder(currentUser) + "/";
//		
//		for (FileDocument file : files) {
//			PublishState state = file.getPublishState(virtuosoConfiguration.getId());
//			
//    		File folder = new File(fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder, file.getId().toString());
//    		
//    		for (File f : folder.listFiles()) {
//    			if (dataset.getType().equals("annotation-dataset")) {
//    				prepare(uf + file.getId().toString(), f.getName(), uploadedFiles);
//    				stmt.execute(lddir(uf + file.getId().toString(), f.getName(), dataset.getAsProperty()));
//    			} else {
//    				prepare(uf + file.getId().toString(), f.getName(), uploadedFiles);
//    				stmt.execute(lddir(uf + file.getId().toString(), f.getName(), SEMAVocabulary.getDataset(dataset.getUuid()).toString()));
//    			}
//    		}
//		}

	}

	//  publicc:  0 : private
	//            1 : public
	//           -1 : current publication state
	public boolean publish(UserPrincipal currentUser, String virtuoso, String id, int publicc, boolean metadata, boolean content, boolean onlyUnpublished) throws Exception {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		
		PublishState ps = dataset.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
		
		boolean isPublic;
		if (publicc == -1) {
			isPublic = (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC)  ? true : false;
		} else {
			isPublic = publicc == 1 ? true : false;
		}
		
		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		List<FileDocument> files = fileRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));

		List<MappingDocument> mappingsToPublish = new ArrayList<>();
		List<FileDocument> filesToPublish = new ArrayList<>();
		
		try {
	        ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
			ps.setPublishState(DatasetState.PUBLISHING);
			
			for (MappingDocument map : mappings) {
				MappingType type = map.getType();
				
				if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
					for (MappingInstance mi : map.getInstances()) {
						PublishState pstate = mi.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
						ExecuteState estate = mi.getExecuteState(fileSystemConfiguration.getId());
						
						if (estate.getExecuteState() == MappingState.EXECUTED) {
							if ((onlyUnpublished && pstate.getPublishState() != DatasetState.PUBLISHED) || !onlyUnpublished) { 
								pstate.setPublishStartedAt(new Date(System.currentTimeMillis()));
								pstate.setPublishState(DatasetState.PUBLISHING);
								
								mappingsToPublish.add(map);
								
								mappingRepository.save(map);
							}
						}
					}
				}
			}
			
			if (content) {
				for (FileDocument fd : files) {
					PublishState pstate = fd.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
					
					if ((onlyUnpublished && pstate.getPublishState() != DatasetState.PUBLISHED) || !onlyUnpublished) {
						pstate.setPublishStartedAt(new Date(System.currentTimeMillis()));
						pstate.setPublishState(DatasetState.PUBLISHING);
	
						filesToPublish.add(fd);
						
						fileRepository.save(fd);
					}
				}
			}
			
			datasetRepository.save(dataset);
			
			virtuosoJDBC.publish(currentUser, virtuoso, dataset, mappingsToPublish, filesToPublish);
			
			if (!dataset.getType().equals("annotation-dataset")) {
				virtuosoJDBC.addDatasetToAccessGraph(currentUser, virtuoso, dataset.getUuid(), isPublic);
			}
			
			ps = dataset.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
	    	
			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
			if (isPublic) {
				ps.setPublishState(DatasetState.PUBLISHED_PUBLIC);
			} else {
				ps.setPublishState(DatasetState.PUBLISHED_PRIVATE);
			}
			
			for (MappingDocument map : mappingsToPublish) {
				for (MappingInstance mi : map.getInstances()) {
					PublishState state = mi.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
					
					state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
					state.setPublishState(DatasetState.PUBLISHED);
				}
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToPublish) {
				PublishState state = fd.getPublishState(virtuosoConfiguration.get(virtuoso).getId());
				
				state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
				state.setPublishState(DatasetState.PUBLISHED);

				fileRepository.save(fd);
			}

			datasetRepository.save(dataset);
			
			logger.info("Publication of " + SEMAVocabulary.getDataset(dataset.getUuid()) + " completed.");
			
			return true;
		} catch (Exception ex) {
			ps.setPublishState(DatasetState.PUBLISHING_FAILED);
			
			for (MappingDocument map : mappingsToPublish) {
				for (MappingInstance mi : map.getInstances()) {
					PublishState state = mi.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

					state.setPublishState(DatasetState.UNPUBLISHED);
				}
				
				mappingRepository.save(map);
			}
			
			for (FileDocument fd : filesToPublish) {
				PublishState state = fd.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

				state.setPublishState(DatasetState.UNPUBLISHED);
				
				fileRepository.save(fd);
			}

			datasetRepository.save(dataset);
			throw ex;
		}				
	}
	
	
	// Metadata but not content should be unpublished only when republishing metadata
	public boolean unpublish(UserPrincipal currentUser, String id, boolean metadata, boolean content) throws Exception {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();

		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}

		ps.setPublishState(DatasetState.UNPUBLISHING);

		
		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		List<FileDocument> files = fileRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		List<MappingDocument> mappingsToUnpublish = new ArrayList<>();
		List<FileDocument> filesToUnpublish = new ArrayList<>();
		
		
		for (MappingDocument map : mappings) {
			MappingType type = map.getType();
			
			if ((metadata && type == MappingType.HEADER) || (content && type == MappingType.CONTENT)) {
				for (MappingInstance mi : map.getInstances()) {
					PublishState state = mi.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

					state.setPublishState(DatasetState.UNPUBLISHING);
					
					mappingsToUnpublish.add(map);
				}
				mappingRepository.save(map);
			}
		}
		
		if (content) {
			for (FileDocument fd : files) {
				PublishState state = fd.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

				state.setPublishState(DatasetState.UNPUBLISHING);
				
				filesToUnpublish.add(fd);

				fileRepository.save(fd);
			}
		}
		
		datasetRepository.save(dataset);
        
		virtuosoJDBC.unpublish(currentUser, virtuoso, dataset, mappingsToUnpublish, filesToUnpublish, metadata, content);
		
		if (content && metadata) {
			if (!dataset.getType().equals("annotation-dataset")) {
				virtuosoJDBC.removeDatasetFromAccessGraph(virtuoso, dataset.getUuid().toString());
			}
		}
		
		if (content && metadata) {
			ps.setPublishState(DatasetState.UNPUBLISHED);
			ps.setPublishStartedAt(null);
			ps.setPublishCompletedAt(null);
//			dataset.removePublishState(ps);
		}
		
		for (MappingDocument map : mappingsToUnpublish) {
			for (MappingInstance mi : map.getInstances()) {
				PublishState state = mi.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

				state.setPublishState(DatasetState.UNPUBLISHED);
				state.setPublishStartedAt(null);
				state.setPublishCompletedAt(null);
//				mi.removePublishState(state);
			}
			mappingRepository.save(map);
		}
		
		for (FileDocument fd : filesToUnpublish) {
			PublishState state = fd.getPublishState(virtuosoConfiguration.get(virtuoso).getId());

			state.setPublishState(DatasetState.UNPUBLISHED);
			state.setPublishStartedAt(null);
			state.setPublishCompletedAt(null);
//			fd.removePublishState(state);

			fileRepository.save(fd);
		}
		
		datasetRepository.save(dataset);
		
		logger.info("Unpublication of " + SEMAVocabulary.getDataset(dataset.getUuid()) + " completed.");
		
		return true;
	}
	
	
	
	public boolean republishMetadata(UserPrincipal currentUser, String id) throws Exception {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();

		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}
		
		int isPublic = -1;
		if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
			isPublic = 1;
		} else if (ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
			isPublic = 0;
		}
		
		if (!unpublish(currentUser, id, true, false)) {
			return false;
		}
		if (!publish(currentUser, virtuoso, id, isPublic, true, false, false)) {
			return false;
		}
		
		return true;
	}
	
	public boolean flipVisibility(UserPrincipal currentUser, String id) throws Exception {
		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
		
		if (!doc.isPresent()) {
			return false;
		}
		
		Dataset dataset = doc.get();
		
		PublishState ps = null;
		String virtuoso = null;

		for (VirtuosoConfiguration vc : virtuosoConfiguration.values()) {
			ps = dataset.checkPublishState(vc.getId());
			if (ps != null) {
				virtuoso = vc.getName();
				break;
			}
		}
		
		if (ps == null) {
			return false;
		}

		if (!dataset.getType().equals("annotation-dataset")) {
			
			if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC) {
				virtuosoJDBC.addDatasetToAccessGraph(currentUser, virtuoso, dataset.getUuid().toString(), false);
				ps.setPublishState(DatasetState.PUBLISHED_PRIVATE);
				
				logger.info("Visibility of " + SEMAVocabulary.getDataset(dataset.getUuid()) + " changed to PRIVATE.");
				
			} else if (ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE) {
				virtuosoJDBC.addDatasetToAccessGraph(currentUser, virtuoso, dataset.getUuid().toString(), true);
				ps.setPublishState(DatasetState.PUBLISHED_PUBLIC);
				
				logger.info("Visibility of " + SEMAVocabulary.getDataset(dataset.getUuid()) + " changed to PUBLIC.");
			} else {
				return false;
			}
			
			datasetRepository.save(dataset);
		}
		
		return true;
		
	}	
	
//	public boolean republishMetadata(UserPrincipal currentUser, String id) throws Exception {
//		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (!doc.isPresent()) {
//			return false;
//		}
//
//		Dataset dataset = doc.get();
//		
//		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//
//		PublishState ps = dataset.getPublishState(virtuosoConfiguration.getId());
//
//		try {
//			
//			DatasetState dsState = ps.getPublishState();
//			
//			ps.setPublishState(DatasetState.UNPUBLISHING);
//			
//			for (MappingDocument map : mappings) {
//				if (map.getType() == MappingType.HEADER) {
//					for (MappingInstance mi : map.getInstances()) {
//						PublishState state = mi.getPublishState(virtuosoConfiguration.getId());
//		
//						state.setPublishState(DatasetState.UNPUBLISHING);
//					}
//					mappingRepository.save(map);
//				}
//			}
//			
//			datasetRepository.save(dataset);
//	        
//			virtuosoJDBC.unpublishMetadata(currentUser, dataset, mappings);
//	
//			ps.setPublishState(DatasetState.UNPUBLISHED);
//			ps.setPublishStartedAt(null);
//			ps.setPublishCompletedAt(null);
//			
//			for (MappingDocument map : mappings) {
//				if (map.getType() == MappingType.HEADER) {
//					for (MappingInstance mi : map.getInstances()) {
//						PublishState state = mi.getPublishState(virtuosoConfiguration.getId());
//		
//						state.setPublishState(DatasetState.UNPUBLISHED);
//						state.setPublishStartedAt(null);
//						state.setPublishCompletedAt(null);
//					}
//					mappingRepository.save(map);
//				}
//			}
//			
//			datasetRepository.save(dataset);
//			
//			System.out.println("UNPUBLICATION COMPLETED");
//					
//	        ps.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(DatasetState.PUBLISHING);
//				
//			for (MappingDocument map : mappings) {
//				if (map.getType() == MappingType.HEADER) {
//					for (MappingInstance mi : map.getInstances()) {
//						PublishState pstate = mi.getPublishState(virtuosoConfiguration.getId());
//						ExecuteState estate = mi.getExecuteState(fileSystemConfiguration.getId());
//						
//						if (estate.getExecuteState() == MappingState.EXECUTED && 
//								(pstate.getPublishState() == DatasetState.UNPUBLISHED || pstate.getPublishState() == DatasetState.PUBLISHING_FAILED)) {
//							pstate.setPublishStartedAt(new Date(System.currentTimeMillis()));
//							pstate.setPublishState(DatasetState.PUBLISHING);
//						}
//					}
//					mappingRepository.save(map);
//				}
//			}
//			
//			datasetRepository.save(dataset);
//		        
//			virtuosoJDBC.republishMetadata(currentUser, dataset, mappings);
//				
//			ps = dataset.getPublishState(virtuosoConfiguration.getId());
//		    	
//			ps.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			ps.setPublishState(dsState);
//				
//			for (MappingDocument map : mappings) {
//				if (map.getType() == MappingType.HEADER) {
//					for (MappingInstance mi : map.getInstances()) {
//						PublishState state = mi.getPublishState(virtuosoConfiguration.getId());
//						
//						if (state.getPublishState() == DatasetState.PUBLISHING) {
//							state.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//							state.setPublishState(DatasetState.PUBLISHED);
//						}
//					}
//					mappingRepository.save(map);
//				}
//			}
//	
//			datasetRepository.save(dataset);
//			
//			System.out.println("PUBLICATION COMPLETED");
//			
//			return true;
//		} catch (Exception ex) {
//			ps.setPublishState(DatasetState.PUBLISHING_FAILED);
//			
//			for (MappingDocument map : mappings) {
//				if (map.getType() == MappingType.HEADER) {
//					for (MappingInstance mi : map.getInstances()) {
//						PublishState state = mi.getPublishState(virtuosoConfiguration.getId());
//	
//						if (state.getPublishState() == DatasetState.PUBLISHING) {
//							state.setPublishState(DatasetState.UNPUBLISHED);
//						}
//					}
//					mappingRepository.save(map);
//				}
//			}
//			
//			datasetRepository.save(dataset);
//			throw ex;
//		}				
//	}	
	
	
    @Value("${mapping.execution.folder}")
    private String mappingsFolder;
    
    @Value("${mapping.uploaded-files.folder}")
    private String uploadsFolder;    


    private String getMappingsFolder(UserPrincipal currentUser) {
    	if (mappingsFolder.endsWith("/")) {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder.substring(0, mappingsFolder.length() - 1);
    	} else {
    		return fileSystemConfiguration.getUserDataFolder(currentUser) + mappingsFolder;
    	}
    }
    
//    private String getUploadsFolder(UserPrincipal currentUser) {
//    	if (uploadsFolder.endsWith("/")) {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder.substring(0, uploadsFolder.length() - 1);
//    	} else {
//    		return fileSystemConfiguration.getUserDataFolder(currentUser) + uploadsFolder;
//    	}    	
//    }
	
//	public boolean checkMetadata(UserPrincipal currentUser, String id) throws Exception {
//		Optional<Dataset> doc = datasetRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (!doc.isPresent()) {
//			return false;
//		}
//
//		List<MappingDocument> mappings = mappingRepository.findByDatasetIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//    	String mf = getMappingsFolder(currentUser);
//
//		Model model = ModelFactory.createDefaultModel();
//
//		for (MappingDocument map : mappings) {
//			if (map.getParameters().isEmpty()) {
//			    if (map.getType() == MappingType.HEADER) {
//					RDFDataMgr.read(model, new StringReader(mf + File.separatorChar + map.getUuid() + ".trig"), null, Lang.TTL);
//			    }
//	    	} else {
//	    		for (MappingInstance mi : map.getInstances()) {
//	    			RDFDataMgr.read(model, new StringReader(mf +  map.getUuid() + "_" + mi.getId().toString() + ".trig"), null, Lang.TTL);
//	    		}
//	    	}
//		}
//		
//		List<String> labels = new ArrayList<>();
//		for (StmtIterator iter = model.listStatements(SEMAVocabulary.getDataset(id), RDFS.label, (RDFNode)null);iter.hasNext();) {
//			Statement stmt = iter.next();
//			labels.add(stmt.getObject().asLiteral().toString());
//		}
//		
//		List<String> labels = new ArrayList<>();
//		for (StmtIterator iter = model.listStatements(SEMAVocabulary.getDataset(id), RDFS.label, (RDFNode)null);iter.hasNext();) {
//			Statement stmt = iter.next();
//			labels.add(stmt.getObject().asLiteral().toString());
//		}
//		
//		
//    	String sparql = "CONSTRUCT { " + 
//		        "  <" + resource + "> <http://www.w3.org/2000/01/rdf-schema#label> ?label " + 
//			        "} WHERE { " + 
//		        " GRAPH <" + SEMAVocabulary.contentGraph + "> { " + 
//		        "   ?g <http://sw.islab.ntua.gr/apollonis/ms/dataProperty> ?p ." + 
//		        "   ?p \" GRAPH <\" + SEMAVocabulary.contentGraph + \"> { \" + . " + 
//		        "   ?p <http://sw.islab.ntua.gr/apollonis/ms/uri> ?uri } " + 
//		        "  GRAPH ?g { <" + resource + "> ?uri ?label }" + 
//		        "}";
//
//System.out.println(sparql);
//
//Writer sw = new StringWriter();
//
//try (QueryExecution qe = QueryExecutionFactory.sparqlService(virtuosoConfiguration.getSparqlEndpoint(), QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//	Model model = qe.execConstruct();
//	
//	RDFDataMgr.write(sw, model, RDFFormat.JSONLD_EXPAND_PRETTY) ;
//}
//		
//		return true;
//	}
}
