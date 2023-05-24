package ac.software.semantic.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.bson.types.ObjectId;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.EmbedderDocument;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.constants.IndexingState;
import ac.software.semantic.model.constants.RDFTermType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.repository.DatasetRepository;
//import ac.software.semantic.repository.IndexRepository;
import ac.software.semantic.repository.IndexStructureRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import ac.software.semantic.vocs.SEMAVocabulary;
import ac.software.semantic.vocs.SEMRVocabulary;
import edu.ntua.isci.ac.semaspace.index.Indexer;

@Service
public class IndexService {

	private static int VIRTOSO_BULK_COUNT = 100;
	
//	@Autowired
//	private ModelMapper modelMapper;
	
	@Autowired
	private SEMRVocabulary resourceVocabulary;
	
//	@Autowired
//	IndexRepository indexRepository;

	@Autowired
	DatasetRepository datasetRepository;

	@Autowired
	IndexStructureRepository indexStructureRepository;

	@Autowired
	SPARQLService sparqlService;
	
    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;

    @Autowired
	@Qualifier("database")
	private Database database;
	
	public IndexStructure createIndex(UserPrincipal currentUser, String indexIdentifier, ElasticConfiguration ec, List<ClassIndexElement> elements, List<IndexKeyMetadata> keysMetadata) {

		Optional<IndexStructure> isOpt = indexStructureRepository.findByDatabaseIdAndIdentifier(database.getId(), indexIdentifier);
		if (isOpt.isPresent()) {
			return null;
		}

		String uuid = UUID.randomUUID().toString();

		IndexStructure is = new IndexStructure();
		is.setUserId(new ObjectId(currentUser.getId()));
		is.setDatabaseId(database.getId());
		is.setUuid(uuid);
		is.setIdentifier(indexIdentifier);
		is.setElements(elements);
		is.setKeysMetadata(keysMetadata);
		is.setUpdatedAt(new Date());
		is.setElasticConfigurationId(ec.getId());

		is = indexStructureRepository.save(is);

		return is;
	}
	
//	public Optional<IndexDocumentResponse> getIndex(UserPrincipal currentUser, String uri) {
//		
//		String datasetUuid = SEMAVocabulary.getId(uri);
//		
//        Optional<IndexDocument> doc = indexRepository.findByDatasetUuidAndHostAndUserId(datasetUuid, elasticConfiguration.getIndexIp(), new ObjectId(currentUser.getId()));
//        if (doc.isPresent()) {
//        	return Optional.of(modelMapper.index2IndexResponse(doc.get()));
//        } else {
//        	return Optional.empty();
//        }
//    }
    
	public List<IndexStructure> getIndices() {
		
	    return indexStructureRepository.findByDatabaseId(database.getId());
	}
	
//	public IndexDocument createIndexDocument(UserPrincipal currentUser, String datasetUri) {
//		
//		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(datasetUri);
//		
//		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
//		if (ds.isPresent()) {
//			
//			String uuid = UUID.randomUUID().toString();
//		
//			IndexDocument d = new IndexDocument();
//			d.setUserId(new ObjectId(currentUser.getId()));
//			d.setDatasetId(ds.get().getId());
//			d.setDatasetUuid(datasetUuid);
//			d.setUuid(uuid);
//			
//			IndexDocument doc = indexRepository.save(d);
//			
//			return doc;
//		} else {
//			return null;
//		}
//	}
	
	
//	public boolean indexCollectionOld(UserPrincipal currentUser, String uri) throws IOException {
//		IndexDocument doc = createIndexDocument(currentUser, uri);
//		
//		doc.setIndexState(IndexingState.INDEXING);
//		doc.setIndexStartedAt(new Date(System.currentTimeMillis()));
//		indexRepository.save(doc);
//		
//		ElasticConfiguration ec = elasticConfigurations.values().iterator().next(); // legacy
//
//		Indexer indexer = new Indexer(virtuosoConfigurations.values().iterator().next().getSparqlEndpoint());
//		
//		indexer.indexCollection(ec.getIndexIp(), ec.getIndexDataName(), uri, null);
//		
//		doc.setIndexState(IndexingState.INDEXED);
//		doc.setIndexCompletedAt(new Date(System.currentTimeMillis()));
//		indexRepository.save(doc);
//		
//		return true;
//	}
	
//	public boolean unindexCollectionOld(UserPrincipal currentUser, String uri) throws IOException {
//		String datasetUuid = resourceVocabulary.getUuidFromResourceUri(uri);
//		
//		Optional<Dataset> dopt = datasetRepository.findByUuidAndUserId(datasetUuid, new ObjectId(currentUser.getId()));
//			
//		if (!dopt.isPresent()) {
//			return false;
//		}
//			
//		Dataset dataset = dopt.get();
//	
//		PublishState ps = null;
//		Indexer indexer = null;
//	
//		for (TripleStoreConfiguration vc : virtuosoConfigurations.values()) {
//			ps = dataset.checkPublishState(vc.getId());
//			if (ps != null) {
//				indexer = new Indexer(vc.getSparqlEndpoint());
//				break;
//			}
//		}
//		
//		if (indexer == null) {
//			return false;
//		}
//	
//		ElasticConfiguration ec = elasticConfigurations.values().iterator().next(); // legacy
//		
//		IndexDocument doc = indexRepository.findByDatasetUuidAndHostAndUserId(datasetUuid, ec.getIndexIp(), new ObjectId(currentUser.getId())).get();
//	
//		doc.setIndexState(IndexingState.UNINDEXING);
//		doc.setIndexStartedAt(new Date(System.currentTimeMillis()));
//		indexRepository.save(doc);
//	
//		indexer.unindexCollection(ec.getIndexIp(), ec.getIndexDataName(), uri);
//	
//		indexRepository.delete(doc);
//			
//		return true;
//		
//	}	
	
	
	public void index(Dataset dataset, ElasticConfiguration ec, TripleStoreConfiguration vc, IndexStructure idxStruct) throws IOException {
		
		try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {

			String graph = resourceVocabulary.getDatasetAsResource(dataset.getUuid()).toString();

			for (ClassIndexElement ie : idxStruct.getElements()) {
				BulkRequestProcessor brp = new BulkRequestProcessor(client);

				String sparql = ie.topElementsListSPARQL(graph); 
				SPARQLStructure ss = sparqlService.toSPARQL(ie);
				
//				List<List<Resource>> paths = ss.getPaths();
//				List<String> keys = new ArrayList<>();
//				for (List<Resource> list : paths) {
//					keys.add(idxStruct.getPathKey(list));
//				}
				List<String> keys = new ArrayList<>(ss.getKeys());

				Map<Integer, IndexKeyMetadata> keyMap = idxStruct.getKeyMetadataMap();

				System.out.println(keys);
				System.out.println(sparql);
				System.out.println(QueryFactory.create(sparql));
	            try (VirtuosoSelectIterator rs = new VirtuosoSelectIterator(vc.getSparqlEndpoint(), sparql)) {
	                
	                while (rs.hasNext()) {
	                	int count = 0;
	                	
	                	List<Resource> subjects = new ArrayList<>();
	                	while (rs.hasNext() && count < VIRTOSO_BULK_COUNT) {
	                		QuerySolution sol = rs.next();
	                		count++;
	                		
	                		Resource subject = sol.getResource("s");
	                		subjects.add(subject);
	                	}
	                    

	            		String subjectSparql = ss.construct("FROM <" + graph + ">", subjects);
	            		
//	            		System.out.println(QueryFactory.create(subjectSparql, Syntax.syntaxARQ));

	                    try (QueryExecution qe = QueryExecutionFactory.sparqlService(vc.getSparqlEndpoint(), QueryFactory.create(subjectSparql, Syntax.syntaxARQ))) {
	                    	Model model = qe.execConstruct();

	                    	for (Resource subject : subjects) {
								brp.addRequest(new IndexRequest(idxStruct.getLocalIdentifier(database, dataset)).source(buildItem(graph, subject, model, ss, keys, keyMap)));
	                    	}
	                    }
					}
								
				}

	            brp.flush();
			}
		}
	}	

	public void unindex(Dataset dataset, ElasticConfiguration ec, String graph) throws IOException {
		
		try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
		   
			String id = IndexStructure.getLocalIdentifier(database, dataset);

			DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(id);
			deleteRequest.setQuery(new TermQueryBuilder("graph", graph));
			
			BulkByScrollResponse bulkResponse = client.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
			
			
			Response resp = client.getLowLevelClient().performRequest(new Request("GET", id + "/_stats"));
			JsonNode body =  new ObjectMapper().readTree(resp.getEntity().getContent());
			int size = body.get("indices").get(id).get("primaries").get("docs").get("count").asInt();

			if (size == 0) {
				DeleteIndexRequest request = new DeleteIndexRequest(id);
				AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
			}
			
		}
	}	

	private class BulkRequestProcessor {
		private BulkRequest bulkRequest;
		private int bulkCount = 0;
		
		private RestHighLevelClient client;
		
		public BulkRequestProcessor(RestHighLevelClient client)	{
			this.client = client;

			bulkRequest = new BulkRequest();
			bulkCount = 0;
		}
		
		public void addRequest(IndexRequest ir) throws IOException {
			bulkRequest.add(ir);
			bulkCount++;
			
//			if (bulkCount % 1000 == 0) {
//				System.out.println(bulkCount);
//			}
			
			if (bulkCount % 10000 == 0) {
				
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				
				if (bulkResponse.hasFailures()) { 
					for (BulkItemResponse bulkItemResponse : bulkResponse) {
					    if (bulkItemResponse.isFailed()) { 
					        System.out.println(bulkItemResponse.getFailure()); 
					    }
					}
				}
				
				bulkRequest = new BulkRequest();
				bulkCount = 0;
			}
		}
		
		public void flush() throws IOException {
			if (bulkCount > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
			
				if (bulkResponse.hasFailures()) { 
					for (BulkItemResponse bulkItemResponse : bulkResponse) {
					    if (bulkItemResponse.isFailed()) { 
					        System.out.println(bulkItemResponse.getFailure()); 
					    }
					}
				}
			}
		}
	}
	
	private XContentBuilder buildItem(String graph, Resource subject, Model model, SPARQLStructure ss, List<String> keys, Map<Integer, IndexKeyMetadata> keyMap) throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		builder.field("ctype", "dataset");
		builder.field("iri", subject.getURI());
	    builder.field("graph", graph);

    	List<List<String>> lexicalForms = new ArrayList<>();
    	List<List<String>> languages = new ArrayList<>();
    	List<List<String>> uris = new ArrayList<>();
    	
    	for (String key : keys) {
    		
    		List<RDFNode> res  = ss.getResultForKey(model, subject, key);
//    		System.out.println(i + " " + res);

        	List<String> lexicalForm = new ArrayList<>();
        	lexicalForms.add(lexicalForm);
        	
        	List<String> language = new ArrayList<>();
        	languages.add(language);
        	
        	List<String> uri = new ArrayList<>();
        	uris.add(uri);
		    	
        	for (RDFNode node : res) {
	    		if (node.isResource()) {
	    			uri.add(node.toString());
	    		} else if (node.isLiteral()) {
	    			Literal lit = node.asLiteral();
	    			lexicalForm.add(lit.getLexicalForm());
	    			String lang = lit.getLanguage();
	    			if (lang != null && lang.length() > 0) {
	    				language.add(lang);
	    			}
	    		}
	    	}

    	}
    	
    	for (int i = 0; i < keys.size(); i++) {
    		String key = keys.get(i);
    		
    		int keyIndex = Integer.parseInt(key.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);

    		String keyName = ikm == null ? key : ikm.getName();
    		
    		String uriSuffix = ikm != null && ikm.getDefaultTermType() == RDFTermType.IRI ? "" : "-uri" ;
    		String lexicalFormSuffix = ikm != null && ikm.getDefaultTermType() == RDFTermType.LITERAL ? "" : "-lexical-form" ;
    		
			if (uris.get(i).size() > 1) {
			    builder.startArray(keyName + uriSuffix);
		    	for (String uri : uris.get(i)) {
	    			builder.value(uri);
		    	}
		    	builder.endArray();
			} else if (uris.get(i).size() == 1) {
    			builder.field(keyName + uriSuffix, uris.get(i).get(0));
    		}
    		
			if (lexicalForms.get(i).size() > 1) {
			    builder.startArray(keyName + lexicalFormSuffix);
		    	for (String lexicalForm : lexicalForms.get(i)) {
	    			builder.value(lexicalForm);
		    	}
		    	builder.endArray();
			    builder.startArray(keyName + lexicalFormSuffix + "-raw");
		    	for (String lexicalForm : lexicalForms.get(i)) {
	    			builder.value(lexicalForm);
		    	}
		    	builder.endArray();
    		} else if (lexicalForms.get(i).size() == 1) {
    			builder.field(keyName + lexicalFormSuffix, lexicalForms.get(i).get(0));
    			builder.field(keyName + lexicalFormSuffix + "-raw", lexicalForms.get(i).get(0));
    		}
			
			if (ikm.isLanguageField()) {
	    		if (languages.get(i).size() > 1) {
				    builder.startArray(keyName + "-lang");
			    	for (String language : languages.get(i)) {
		    			builder.value(language);
			    	}
			    	builder.endArray();
	    		} else if (languages.get(i).size() == 1) {
	    			builder.field(keyName + "-lang", languages.get(i).get(0));
	    		}
			}
    	}
    	
		builder.endObject();
		
//		String json = Strings.toString(builder);
//		System.out.println(json);

		return builder;
		
	}
	

	


	
//	public boolean doIndex(UserPrincipal currentUser, String uri, List<String> path) throws IOException  {
//		
//		IndexDocument doc = createIndexDocument(currentUser, uri, path);
//		
//		doc.setIndexState(IndexingState.INDEXING);
//		doc.setIndexStartedAt(new Date(System.currentTimeMillis()));
//		indexRepository.save(doc);
//
//		String spath = "";
//		for (int i = 0; i < path.size(); i++) {
//			if (i > 0) {
//				spath +="/";
//			}
//			spath += "<" + path.get(i) + ">";
//		}
//		
//		try(RestHighLevelClient client = new RestHighLevelClient(
//		        RestClient.builder(
//		                new HttpHost(ApollonisSources.ELASTIC, 9200, "http")))) {
//		
//			BulkRequest bulkRequest = new BulkRequest();
//			
//			String sparql = 
//					"SELECT ?s ?value " +
//			        "WHERE { " +
//					"  GRAPH <" + uri + "> { " +
//			        "    ?s " + spath + " ?value } } " + 
//					"ORDER BY ?s ";
//			
//	
//			XContentBuilder builder = null;
//			String item = null;
//			
//			List<String> value = new ArrayList<>();
//			List<String> valueAnalyzed = new ArrayList<>();
//			
//			int bulkCount = 0;
//			
//			int iter = 1;
//			String offset = "";
//			
//			while (true) {
//				QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql + offset, Syntax.syntaxARQ));
//				
//				ResultSet rs = qe.execSelect();
//				
//				int sparqlCount = 0;
//				
//				while (rs.hasNext()) {
//					sparqlCount++;
//	
//					QuerySolution sol = rs.next();
//					
//					String newItem = sol.get("s").toString();
//					
//					if (item == null || !item.equals(newItem)) {
//						if (item != null) {
//							builder.array("value", value.toArray(new String[] {}));
//							builder.array("value_analyzed", valueAnalyzed.toArray(new String[] {}));
//
////							System.out.println(builder);
//							builder.endObject();
//							
//							bulkRequest.add(new IndexRequest(indexName).source(builder));
//							bulkCount++;
//							
//							if (bulkCount % 10000 == 0) {
//								BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//								
//								if (bulkResponse.hasFailures()) { 
//									for (BulkItemResponse bulkItemResponse : bulkResponse) {
//									    if (bulkItemResponse.isFailed()) { 
//									        System.out.println(bulkItemResponse.getFailure()); 
//						
//									    }
//									}
//								}
//								
//								bulkRequest = new BulkRequest();
//								bulkCount = 0;
//							}
//						}
//						
//						item = newItem;
////						System.out.println(item);
//						
//						builder = XContentFactory.jsonBuilder();
//						builder.startObject();
//					
//						builder.field("ctype", "dataset");
//					    builder.field("graph", uri);
//					    builder.field("item", item);
//					    builder.field("property", spath);
//					    
//					    value = new ArrayList<>();
//					    valueAnalyzed = new ArrayList<>();
//					}
//					
//					String v = sol.get("value").toString();
//					
//					value.add(v);
//					valueAnalyzed.add(v);
//					
//				}
//				
//				if (sparqlCount == VIRTUOSO_LIMIT) {
//					offset = " OFFSET " + iter*VIRTUOSO_LIMIT;
//					iter++;
//				} else {
//					break;
//				}
//			}
//			
//			if (builder != null) {
//				builder.array("value", value.toArray(new String[] {}));
//				builder.array("value_analyzed", valueAnalyzed.toArray(new String[] {}));
//				
//				builder.endObject();
//			}
//			
//			
//			if (bulkCount > 0) {
//				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//			
//				if (bulkResponse.hasFailures()) { 
//					for (BulkItemResponse bulkItemResponse : bulkResponse) {
//					    if (bulkItemResponse.isFailed()) { 
//					        System.out.println(bulkItemResponse.getFailure()); 
//					    }
//					}
//				}
//			}
//		}
//		
//		doc.setIndexState(IndexingState.INDEXED);
//		doc.setIndexCompletedAt(new Date(System.currentTimeMillis()));
//		indexRepository.save(doc);
//		
//		return true;
//	} 

//	public boolean doUnindex(UserPrincipal currentUser, String uri, List<String> path) throws IOException  {
//
//		String datasetUuid = SEMAVocabulary.getId(uri);
//		
//		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
//		if (ds.isPresent()) {
//		
//			IndexDocument doc = indexRepository.findByDatasetUuidAndOnPropertyAndUserId(datasetUuid, path.toArray(new String[] {}), new ObjectId(currentUser.getId())).get();
//
//			doc.setIndexState(IndexingState.UNINDEXING);
//			doc.setIndexStartedAt(new Date(System.currentTimeMillis()));
//			indexRepository.save(doc);
//	
//			String spath = "";
//			for (int i = 0; i < path.size(); i++) {
//				if (i > 0) {
//					spath +="/";
//				}
//				spath += "<" + path.get(i) + ">";
//			}
//			
//			
//			try(RestHighLevelClient client = new RestHighLevelClient(
//			        RestClient.builder(
//			                new HttpHost(ApollonisSources.ELASTIC, 9200, "http")))) {
//			
//				SearchRequest searchRequest = new SearchRequest(indexName); 
//				SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//				
//				BoolQueryBuilder bool = QueryBuilders.boolQuery();
//				bool.must(QueryBuilders.termQuery("ctype", "dataset"));
//				bool.must(QueryBuilders.termQuery("graph", uri));
//				bool.must(QueryBuilders.termQuery("property", spath));
//						
//				searchSourceBuilder.query(bool);
//				searchSourceBuilder.size(elasticScrollSize);
//				
//				searchRequest.source(searchSourceBuilder);
//				searchRequest.scroll(TimeValue.timeValueMinutes(1L));
//				
//				SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//	
//				String scrollId = searchResponse.getScrollId();
//				SearchHit[] sh = searchResponse.getHits().getHits();
//	
//				List<String> ids = new ArrayList<>();
//				
//				for (SearchHit s : sh) {
//					ids.add(s.getId());
//				}
//				
//				
//				int count = sh.length;
//				
//				while (sh.length == elasticScrollSize) {
//					SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
//					scrollRequest.scroll(TimeValue.timeValueSeconds(30));
//					SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
//					scrollId = searchScrollResponse.getScrollId();  
//					
//					sh = searchScrollResponse.getHits().getHits();
//					
//					for (SearchHit s : sh) {
//						ids.add(s.getId());
//					}
//	
//					
//					count += sh.length;
//	//				System.out.println(sh.length);
//				}
//	
//				BulkRequest bulkRequest = new BulkRequest();
//				
//				int bulkCount = 0;
//				
//				for (String id : ids) {
//					bulkRequest.add(new DeleteRequest(indexName, id));
//					bulkCount++;
//					
//					if (bulkCount % 10000 == 0) {
//						BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//									
//						if (bulkResponse.hasFailures()) { 
//							for (BulkItemResponse bulkItemResponse : bulkResponse) {
//							    if (bulkItemResponse.isFailed()) { 
//							        System.out.println(bulkItemResponse.getFailure()); 
//				
//							    }
//							}
//						}
//						
//						bulkRequest = new BulkRequest();
//						bulkCount = 0;
//					}
//				}
//				
//				if (bulkCount > 0) {
//					BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//				
//					if (bulkResponse.hasFailures()) { 
//						for (BulkItemResponse bulkItemResponse : bulkResponse) {
//						    if (bulkItemResponse.isFailed()) { 
//						        System.out.println(bulkItemResponse.getFailure()); 
//						    }
//						}
//					}
//				}
//			}
//			
//			indexRepository.delete(doc);
//			
//			return true;
//		}
//		
//		return false;
//		
//	} 	
	
//	public static void getInTimes(Resource time, Set<Resource> times) {
//		try (QueryExecution qet = QueryExecutionFactory.sparqlService(ApollonisSources.RDF_STORE, 
//				 QueryFactory.create("SELECT DISTINCT ?q WHERE { <" + time + "> <http://www.w3.org/2006/time#inside>?/<http://www.w3.org/2006/time#intervalIn>* ?q } "))) { 
//	
//			ResultSet rst = qet.execSelect();
//	
//			while (rst.hasNext()) {
//				QuerySolution sol = rst.next();
//				times.add(sol.get("q").asResource());
//			}
//		}
//	}		
	
//	public boolean doIndex(UserPrincipal currentUser, AnnotatorDocument adoc) throws IOException  {
//		
//		try(RestHighLevelClient client = new RestHighLevelClient(
//		        RestClient.builder(
//		                new HttpHost(ApollonisSources.ELASTIC, 9200, "http")))) {
//		
//			BulkRequest bulkRequest = new BulkRequest();
//			
//			String property = adoc.getAsProperty();
//			
//			if (property.equals("http://sw.islab.ntua.gr/semaspace/ontology/time")) {
//				
//				Set<Resource> times = new HashSet<>();
//				
//				String sparql31 = "SELECT ?item ?time ?startTime ?endTime " +
//		                 "WHERE  {" +
//						    "GRAPH <" + SEMAVocabulary.annotationGraph + "> {" +
//		                    " ?aset <http://purl.org/dc/terms/hasPart> ?tann " +
//			                "}" +
//		                    "GRAPH <http://sw.islab.ntua.gr/semaspace/ontology/time> { " +
//		                    "  ?tann <http://www.w3.org/ns/oa#hasTarget>/<http://www.w3.org/ns/oa#hasSource> ?item . " + 
//		                    "  ?tann <http://www.w3.org/ns/oa#hasBody> ?time . " +
//		                    "  OPTIONAL { " +
//		                    "     ?tann <http://www.w3.org/ns/oa#hasBody> ?t . " +
//		                    "     ?t <http://www.w3.org/2006/time#intervalStartedBy>|<http://www.w3.org/2006/time#hasBeginning> ?startTime . " +
//		                    "     ?t <http://www.w3.org/2006/time#intervalFinishedBy>|<http://www.w3.org/2006/time#hasEnd> ?endTime .  } } " +
//		                 "} " +
//		                 "ORDER BY ?item";
//				
//				XContentBuilder builder = null;
//				String item = null;
//				
//				int bulkCount = 0;
//				
//				int iter = 1;
//				String offset = "";
//				
//				SearchRequest searchRequest = new SearchRequest(indexName);
//				String datasetGraph = SEMAVocabulary.getDataset(adoc.getDatasetUuid()).toString();
//				
//				while (true) {
//					try (QueryExecution qe31 = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql31 + offset, Syntax.syntaxARQ))) {
//						ResultSet rs31 = qe31.execSelect();
//						
//						int sparqlCount = 0;
//						
//						while (rs31.hasNext()) {
//							sparqlCount++;
//							
//							QuerySolution qs31 = rs31.next();
//							
//							String newItem = qs31.get("?item").toString();
//							
//							if (item == null || !item.equals(newItem)) {
//								if (item != null) {
//									List<String> sTimes = new ArrayList<>();
//									for (Resource t : times) {
//										sTimes.add(t.getURI().toString());
//									}
//									
//									builder.array("time", sTimes.toArray(new String[] {}));
//
////									System.out.println(builder);
//									builder.endObject();
//									
//									SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//									
//									BoolQueryBuilder bool = QueryBuilders.boolQuery();
//									bool.must(QueryBuilders.termQuery("ctype", "annotation"));
//									bool.must(QueryBuilders.termQuery("graph", datasetGraph));
//									bool.must(QueryBuilders.termQuery("item", item));
//											
//									searchSourceBuilder.query(bool);
//									
//									searchRequest.source(searchSourceBuilder);
//									
//									SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//									SearchHit[] sh = searchResponse.getHits().getHits();
//									if (sh.length == 0) {
//										bulkRequest.add(new IndexRequest(indexName).source(builder));
//									} else {
//										bulkRequest.add(new UpdateRequest(indexName, sh[0].getId()).doc(builder));
//									}
//									
//									bulkCount++;
//									
//									if (bulkCount % 10000 == 0) {
//										BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//										
//										if (bulkResponse.hasFailures()) { 
//											for (BulkItemResponse bulkItemResponse : bulkResponse) {
//											    if (bulkItemResponse.isFailed()) { 
//											        System.out.println(bulkItemResponse.getFailure()); 
//								
//											    }
//											}
//										}
//										
//										bulkRequest = new BulkRequest();
//										bulkCount = 0;
//									}
//								}
//								
//								item = newItem;
////								System.out.println(item);
//								
//								builder = XContentFactory.jsonBuilder();
//								builder.startObject();
//							
//								builder.field("ctype", "annotation");
//							    builder.field("graph", datasetGraph);
//							    builder.field("item", item);
//							    
//							    times = new HashSet<>();
//							}
//							
//							Resource time = qs31.get("time").asResource();
//
//							if (!time.isAnon()) {
//								getInTimes(time, times);
//							} else {
//								
//								Resource stime = qs31.get("startTime").asResource();
//								Resource etime = qs31.get("endTime").asResource();
//							
//								for (Resource res : Resolve.coverRange(stime, etime)) {
//									getInTimes(res, times);
//								}
//							}
//						}
//						
//						if (sparqlCount == VIRTUOSO_LIMIT) {
//							offset = " OFFSET " + iter*VIRTUOSO_LIMIT;
//							iter++;
//						} else {
//							break;
//						}
//					}
//					
//					if (builder != null) {
//						List<String> sTimes = new ArrayList<>();
//						for (Resource t : times) {
//							sTimes.add(t.getURI().toString());
//						}
//						
//						builder.array("time", sTimes.toArray(new String[] {}));
//						
//						builder.endObject();
//					}
//							
//							
//				}
//			
//	
//			}				
//			
//		}
//		
//		
//		return true;
//	} 

//	public boolean doUnindexAnnotation(UserPrincipal currentUser, String datasetUri, String property) throws IOException  {
//
//		String datasetUuid = SEMAVocabulary.getId(datasetUri);
//		
//		Optional<Dataset> ds = datasetRepository.findByUuid(datasetUuid);
//		if (ds.isPresent()) {
//		
////			IndexDocument doc = indexRepository.findByDatasetUuidAndOnPropertyAndUserId(datasetUuid, path.toArray(new String[] {}), new ObjectId(currentUser.getId())).get();
////
////			doc.setIndexState(IndexingState.UNINDEXING);
////			doc.setIndexStartedAt(new Date(System.currentTimeMillis()));
////			indexRepository.save(doc);
//	
////			String spath = "";
////			for (int i = 0; i < path.size(); i++) {
////				if (i > 0) {
////					spath +="/";
////				}
////				spath += "<" + path.get(i) + ">";
////			}
//			
//			try(RestHighLevelClient client = new RestHighLevelClient(
//			        RestClient.builder(
//			                new HttpHost(ApollonisSources.ELASTIC, 9200, "http")))) {
//			
//				SearchRequest searchRequest = new SearchRequest(indexName); 
//				SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//				
//				BoolQueryBuilder bool = QueryBuilders.boolQuery();
//				bool.must(QueryBuilders.termQuery("ctype", "dataset"));
//				bool.must(QueryBuilders.termQuery("graph", datasetUri));
//				bool.must(QueryBuilders.termQuery("property", property));
//						
//				searchSourceBuilder.query(bool);
//				searchSourceBuilder.size(elasticScrollSize);
//				
//				searchRequest.source(searchSourceBuilder);
//				searchRequest.scroll(TimeValue.timeValueMinutes(1L));
//				
//				SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//	
//				String scrollId = searchResponse.getScrollId();
//				SearchHit[] sh = searchResponse.getHits().getHits();
//	
//				List<String> ids = new ArrayList<>();
//				
//				for (SearchHit s : sh) {
//					ids.add(s.getId());
//				}
//				
//				
//				int count = sh.length;
//				
//				while (sh.length == elasticScrollSize) {
//					SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
//					scrollRequest.scroll(TimeValue.timeValueSeconds(30));
//					SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
//					scrollId = searchScrollResponse.getScrollId();  
//					
//					sh = searchScrollResponse.getHits().getHits();
//					
//					for (SearchHit s : sh) {
//						ids.add(s.getId());
//					}
//	
//					
//					count += sh.length;
//	//				System.out.println(sh.length);
//				}
//	
//				BulkRequest bulkRequest = new BulkRequest();
//				
//				int bulkCount = 0;
//				
//				for (String id : ids) {
//					bulkRequest.add(new DeleteRequest(indexName, id));
//					bulkCount++;
//					
//					if (bulkCount % 10000 == 0) {
//						BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//									
//						if (bulkResponse.hasFailures()) { 
//							for (BulkItemResponse bulkItemResponse : bulkResponse) {
//							    if (bulkItemResponse.isFailed()) { 
//							        System.out.println(bulkItemResponse.getFailure()); 
//				
//							    }
//							}
//						}
//						
//						bulkRequest = new BulkRequest();
//						bulkCount = 0;
//					}
//				}
//				
//				if (bulkCount > 0) {
//					BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//				
//					if (bulkResponse.hasFailures()) { 
//						for (BulkItemResponse bulkItemResponse : bulkResponse) {
//						    if (bulkItemResponse.isFailed()) { 
//						        System.out.println(bulkItemResponse.getFailure()); 
//						    }
//						}
//					}
//				}
//			}
//			
//			indexRepository.delete(doc);
//			
//			return true;
//		}
//		
//		return false;
//		
//	} 	

////	@Autowired
////	private Environment env;
////	
////	private static String currentTime() {
////		SimpleDateFormat srcFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");            
////        return srcFormatter.format(new Date());
////	}
////	
//	public Optional<String> getLastExecution(UserPrincipal currentUser, String id) throws IOException {
//
////      List<MappingExecution> docs = executionsRepository.findLatestByUserIdAndD2rmlId(new ObjectId(currentUser.getId()), new ObjectId(mappingId));
//		Optional<VocabularizerDocument> entry = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//      if (entry.isPresent()) {
//    	  VocabularizerDocument doc = entry.get();
//      	
//      		Path path = Paths.get(vocabularizersFolder + doc.getUuid() + ".trig");
//	      	String file = new String(Files.readAllBytes(path));
//	      	
//	      	return Optional.of(file);
//	      } else {
//	      	return Optional.empty();
//	      }
//  }	
//	
//	public boolean executeVocabularizer(UserPrincipal currentUser, String id) throws Exception {
////		
//		Optional<VocabularizerDocument> res = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//
//		if (res.isPresent()) {
//
//			VocabularizerDocument adoc = res.get();
//
//			try {
//				
//				Optional<Dataset> dres = datasetRepository.findByUuid(adoc.getDatasetUuid());
//				
//				if (dres.isPresent()) {
//					Dataset ds = dres.get();
//
//					adoc.setExecuteState(MappingState.EXECUTING);
//					adoc.setExecuteStartedAt(new Date(System.currentTimeMillis()));
//					vocabularizerRepository.save(adoc);
//
//					List<String> onProperty = adoc.getOnProperty();
//					
//					String spath = "";
//					for (int i = onProperty.size() - 1; i >= 0; i--) {
//						if (i < onProperty.size() - 1) {
//							spath += "/";
//						}
//						spath += "<" + onProperty.get(i) + ">";
//					}
//
//					String sparql = 
//							"SELECT DISTINCT ?value " +
//					        "WHERE { " +
//							"  GRAPH <" + SEMAVocabulary.getDataset(ds.getUuid()) + "> { " +
//					        "    ?s " + spath + " ?value } } ";
//
//					
//					Set<String> values = new HashSet<>();
//					
//					try (RDFSource ts = new RDFJenaModelSource()) {
//					
//						RDFJenaConnection conn = (RDFJenaConnection)ts.getConnection();
//						
//						try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql, Syntax.syntaxARQ))) {
//						
//							ResultSet rs = qe.execSelect();
//							
//							Model model = ModelFactory.createDefaultModel();
//							
//							if (adoc.getSeparator() != null && adoc.getSeparator().length() > 0) {
//								while (rs.hasNext()) {
//									QuerySolution sol = rs.next();
//									
//									String[] vv = sol.get("value").asLiteral().getLexicalForm().split(adoc.getSeparator());
//									
//									for (String v : vv) {
//										v = v.trim();
//										if (v.length() > 0) {
//										
//											if (values.add(v)) {
//												Resource subj = SEMAVocabulary.getTerm(adoc.getUuid() + "#" + UUID.randomUUID().toString());
//											
//												conn.add(subj, RDFVocabulary.type, SKOSVocabulary.Concept);
//												conn.add(subj, SKOSVocabulary.prefLabel, model.createLiteral(v));
//											}
//										}
//									}
//									
//								}
//							} else {
//								while (rs.hasNext()) {
//									QuerySolution sol = rs.next();
//									
//									RDFNode v = sol.get("value");
//									
//									Resource subj = SEMAVocabulary.getTerm(adoc.getUuid() + "#" + UUID.randomUUID().toString());
//									
//									conn.add(subj, RDFVocabulary.type, SKOSVocabulary.Concept);
//									conn.add(subj, SKOSVocabulary.prefLabel, v);
//								}
//							}
//						}
//						
//						conn.saveAsTRIG(vocabularizersFolder, adoc.getUuid());
//					}
//	
//					
//					
//					D2RMLModel rmlMapping = D2RMLModel.readFromFile(vocabularizerD2RML);
//			        
//					Map<String, String> params = new HashMap<>();
//					params.put("id", SEMAVocabulary.getDataset(adoc.getUuid()).toString());
//					params.put("identifier", SEMAVocabulary.getDataset(adoc.getUuid()).toString());
//					params.put("name", adoc.getName());
//					params.put("source", SEMAVocabulary.getDataset(adoc.getDatasetUuid()).toString());
//					params.put("prefix", SEMAVocabulary.getTerm(adoc.getUuid()) + "#");
//	        		if (adoc.getSeparator() != null && adoc.getSeparator().length() > 0) {
//	        			params.put("separator", adoc.getSeparator());
//	        		} else {
//	        			params.put("separator", "");
//	        		}
//	        		
//					RDFSource ts = new RDFJenaModelSource();
//				    Executor executor = new Executor(ts);
//					executor.execute(rmlMapping, params);
//					
//			        RDFJenaConnection conn = (RDFJenaConnection)ts.getConnection();
//	
//			        Model model = conn.getDataset().getDefaultModel();
//			        
//			        // D2RML DOES NOT SUPPORT LISTS
//					RDFNode[] props = new RDFNode[adoc.getOnProperty().size()]; 
//	        		for (int i = adoc.getOnProperty().size() -1; i >= 0; i--) {
//	        			props[adoc.getOnProperty().size()-i-1] =  model.createResource(adoc.getOnProperty().get(i));
//	        		}
//	        		conn.add(SEMAVocabulary.getDataset(adoc.getUuid()), SEMAVocabulary.onProperty, model.createList(props));
//	        		
//			        conn.saveAsTRIG(vocabularizersFolder, adoc.getUuid() + "_catalog");
//					
//					
////					
////			        
////					Model model = ModelFactory.createDefaultModel();
//////					model.add(SEMAVocabulary.getDataset(adoc.getUuid()), RDFVocabulary.type, model.createResource("http://www.w3.org/ns/dcat#Dataset"));
//////					model.add(SEMAVocabulary.getDataset(adoc.getUuid()), RDFVocabulary.type, SEMAVocabulary.VocabularyCollection);
////					model.add(SEMAVocabulary.getDataset(adoc.getUuid()), RDFSVocabulary.label, model.createLiteral(adoc.getName()));
////					model.add(SEMAVocabulary.getDataset(adoc.getUuid()), SEMAVocabulary.source, SEMAVocabulary.getDataset(adoc.getDatasetUuid()));
//////					model.add(SEMAVocabulary.getDataset(adoc.getUuid()), model.createProperty("http://sw.islab.ntua.gr/apollonis/ms/class"), 
//////							
//////							);
//////					
//////					Resource anon = model.createResource();
//////					model.add(anon, RDFVocabulary.type, )
////
////					
////					RDFNode[] props = new RDFNode[adoc.getOnProperty().size()]; 
////	        		for (int i = adoc.getOnProperty().size() -1; i >= 0; i--) {
////	        			props[adoc.getOnProperty().size()-i-1] =  model.createResource(adoc.getOnProperty().get(i));
////	        		}
////	        		
////	        		model.add(SEMAVocabulary.getDataset(adoc.getUuid()), SEMAVocabulary.onProperty, model.createList(props));
////	        		
////
////	        		if (adoc.getSeparator() != null && adoc.getSeparator().length() > 0) {
////	        			model.add(SEMAVocabulary.getDataset(adoc.getUuid()), SEMAVocabulary.separator, model.createLiteral(adoc.getSeparator()));
////	        		}
////	        		
////	        		try (Writer sw = new OutputStreamWriter(new FileOutputStream(new File(vocabularizersFolder + adoc.getUuid()  + "_catalog.trig"), false), StandardCharsets.UTF_8)) {
////	        			RDFDataMgr.write(sw, model, RDFFormat.TRIG);
////	        		}
//		        	
//					adoc.setExecuteCompletedAt(new Date(System.currentTimeMillis()));
//					adoc.setExecuteState(MappingState.EXECUTED);
//					
//					vocabularizerRepository.save(adoc);
//		        	
//			        return true;
//				}
//			}  catch (Exception ex) {
//				adoc.setExecuteState(MappingState.EXECUTION_FAILED);
//		        
//				vocabularizerRepository.save(adoc);
//				
//				throw ex;
//			}
//		}
//		
//		return false;
//
//		
//	}
//	
//	public IndexDocumentResponse getIndexes(UserPrincipal currentUser, String datasetUri) {
//		
//		String datasetUuid = SEMAVocabulary.getId(datasetUri);
//		
//		Optional<IndexDocument> doc = indexRepository.findByDatasetUuidAndHostAndUserId(datasetUuid, elasticConfiguration.getIndexIp(), new ObjectId(currentUser.getId()));
//		
//		if (doc.isPresent()) {
//			return modelMapper.index2IndexResponse(doc.get());
//		} else {
//			return null;
//		}
//	}
	
//	public boolean publish(UserPrincipal currentUser, String id) throws Exception {
//		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (doc.isPresent()) {
//			VocabularizerDocument adoc = doc.get();
//		
//			adoc.setPublishState(DatasetState.PUBLISHING);
//			adoc.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			
//			vocabularizerRepository.save(adoc);
//		
//			virtuosoJDBC.publish(adoc);
//			virtuosoJDBC.addDatasetToAccessGraph(currentUser, adoc.getUuid(), false);
//	    	
//			adoc.setPublishCompletedAt(new Date(System.currentTimeMillis()));
//			adoc.setPublishState(DatasetState.PUBLISHED);
//			
//			vocabularizerRepository.save(adoc);
//			
//			System.out.println("PUBLICATION COMPLETED");
//		}
//		
//		return true;
//	}
//
//
//	public boolean unpublish(UserPrincipal currentUser, String id) throws Exception {
//		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (doc.isPresent()) {
//			VocabularizerDocument adoc = doc.get();
//			
//			adoc.setPublishState(DatasetState.UNPUBLISHING);
//			adoc.setPublishStartedAt(new Date(System.currentTimeMillis()));
//			vocabularizerRepository.save(adoc);
//		
//			virtuosoJDBC.unpublish(adoc);
//			virtuosoJDBC.removeDatasetFromAccessGraph(adoc.getUuid().toString());
//		
//			adoc.setPublishState(DatasetState.UNPUBLISHED);
//
//			adoc.setPublishStartedAt(null);
//			adoc.setPublishCompletedAt(null);
//			
//			vocabularizerRepository.save(adoc);
//		}
//		
//		System.out.println("UNPUBLICATION COMPLETED");
//		
//		return true;
//	}
//	
//	public boolean deleteVocabularizer(UserPrincipal currentUser, String id) {
//		Optional<VocabularizerDocument> doc = vocabularizerRepository.findByIdAndUserId(new ObjectId(id), new ObjectId(currentUser.getId()));
//		
//		if (doc.isPresent()) {
//			VocabularizerDocument adoc = doc.get();
//			
//			try {
//				new File(vocabularizersFolder + adoc.getUuid() + ".trig").delete();
//        	} catch (Exception e) {
//        		e.printStackTrace();
//        	}	
//			try {
//				new File(vocabularizersFolder + adoc.getUuid() + "_catalog.trig").delete();
//        	} catch (Exception e) {
//        		e.printStackTrace();
//        	}			
//			
//			vocabularizerRepository.delete(adoc);
//			
//			return true;
//		}
//
//		return false;
//		
//	}
}
