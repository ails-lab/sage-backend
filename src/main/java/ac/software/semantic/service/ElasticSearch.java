package ac.software.semantic.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ac.software.semantic.config.ConfigurationContainer;
//import ac.software.semantic.config.VocabulariesBean;
import ac.software.semantic.model.Database;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.DatasetCatalog;
import ac.software.semantic.model.DatasetContext;
import ac.software.semantic.model.ElasticConfiguration;
import ac.software.semantic.model.IndexStructure;
import ac.software.semantic.model.RemoteTripleStore;
import ac.software.semantic.model.ResourceContext;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.VocabularyContainer;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.ExpandIndexKeyMetadata;
import ac.software.semantic.model.index.ExpandType;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.repository.core.IndexStructureRepository;
import ac.software.semantic.service.DatasetService.DatasetContainer;
import ac.software.semantic.service.SPARQLService.SPARQLStructure;
import ac.software.semantic.service.monitor.IndexMonitor;
import edu.ntua.isci.ac.common.db.rdf.VectorDatatype;
import edu.ntua.isci.ac.common.db.rdf.VirtuosoSelectIterator;
import edu.ntua.isci.ac.d2rml.vocabulary.lucene.TokenAnalyzer;
import edu.ntua.isci.ac.lod.vocabularies.SKOSVocabulary;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

@Service
public class ElasticSearch {

	private Logger logger = LoggerFactory.getLogger(MappingService.class);
	
	private static int VIRTOSO_BULK_COUNT = 100; // too slow -- errors with more!!!!

    @Autowired
	@Qualifier("database")
	private Database database;
    
    @Autowired
    @Qualifier("triplestore-configurations")
    private ConfigurationContainer<TripleStoreConfiguration> virtuosoConfigurations;
	
	@Autowired
	private IndexStructureRepository indexStructureRepository;

	@Autowired
	private SPARQLService sparqlService;

	@Lazy
	@Autowired
	private DatasetService datasetService;

    @Autowired
    @Qualifier("elastic-configurations")
    private ConfigurationContainer<ElasticConfiguration> elasticConfigurations;

//	@Autowired
//	@Qualifier("prefixes")
//	private SimpleTrie<URIDescriptor> prefixesTrie;
	
//	@Autowired
//	@Qualifier("vocabularies")
//	private VocabulariesBean vocs;
	
	@Autowired
	@Qualifier("skos-cache")
	private Cache skosCache;
	
	@Autowired
	private SchemaService schemaService;
	
	public List<IndexStructure> getIndices() {
	    return indexStructureRepository.findByDatabaseId(database.getId());
	}
	
	public void index(Dataset dataset, ElasticConfiguration ec, IndexStructure idxStruct, IndexMonitor im) throws Exception {

		TypeMapper.getInstance().registerDatatype(new VectorDatatype());
		
		String indexName = getLocalIdentifier(database, dataset, idxStruct);
		
		ElasticsearchClient client = ec.getClient();
		
		DatasetCatalog dcg = schemaService.asCatalog(dataset);
		
    	// get triple store of first dataset ... wrong!
		String endpoint = dataset.getSparqlEndpoint(virtuosoConfigurations.values());
    	String fromClause = schemaService.buildFromClause(dcg, true);
    	
		Map<String, ExpandIndexKeyMetadata> expandTypes = new HashMap<>();
		List<IndexKeyMetadata> keyMetadata = idxStruct.getKeysMetadata();
		for (IndexKeyMetadata km : keyMetadata) {
			ExpandIndexKeyMetadata expand = km.getVexpand();
			if (expand != null) {
				VocabularyContainer<ResourceContext> vcont = new VocabularyContainer<ResourceContext>();
				expand.setVocabularContainer(datasetService.createVocabularyContainer(expand.getDatasetId(), vcont));
				expandTypes.put("r" + km.getIndex(), expand);
			}
		}
		
		Map<Integer, IndexKeyMetadata> keyMap = idxStruct.getKeyMetadataMap();
		
		for (ClassIndexElement ie : idxStruct.getElements()) {
			im.startElement(ie);
			
			String sparql = ie.topElementsListSPARQL(fromClause); 
			SPARQLStructure ss = sparqlService.toSPARQL(ie, keyMap, true);
			
//			System.out.println( ss.getWhereClause());
//			System.out.println(QueryFactory.create(sparql));
			
			List<String> keys = new ArrayList<>(ss.getKeys());

            try (VirtuosoSelectIterator rs = new VirtuosoSelectIterator(endpoint, sparql)) {
                
                while (rs.hasNext()) {
                	int count = 0;
                	
//                	boolean ok = false;
                	List<Resource> subjects = new ArrayList<>();
                	while (rs.hasNext() && count < VIRTOSO_BULK_COUNT) {
						if (Thread.interrupted()) {
							Exception ex = new InterruptedException("The task was interrupted.");
							throw ex;
						}
						
                		im.incrementCurrentCount();
                		
                		QuerySolution sol = rs.next();
                		count++;
                		
                		Resource subject = sol.getResource("s");
                		subjects.add(subject);
                		
//                		if (subject.toString().equals("http://data.europa.eu/s66/resource/organisations/52f7fb09-a503-3d75-ab24-e354f7ceb5de")) {
//                			ok = true;
//                		}
                	}
                    
            		String subjectSparql = ss.construct(fromClause, subjects);

//            		if (ok) {
            			System.out.println(QueryFactory.create(subjectSparql, Syntax.syntaxARQ));
//            		}

            		BulkRequest.Builder br = new BulkRequest.Builder();
            		for (int i = 0; i < 3; i++) {
	                    try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(subjectSparql, Syntax.syntaxARQ))) {
	                    	Model model = qe.execConstruct();
	                    	
	                    	for (Resource subject : subjects) {
	                    		ByteArrayOutputStream bos = buildItem(null, subject, model, ss, keys, keyMap, expandTypes);
	                    	
//	                    		if (ok) {
//	                    			System.out.println(new String(bos.toByteArray()));
//	                    		}
	                       		JsonData jsonData = JsonData.fromJson(new String(bos.toByteArray()));
	                        		
	                       		br.operations(op -> op.index(idx -> idx.index(indexName).document(jsonData)));
	                    	}
	                    	
//	                    	br.build();
	                    
	                    	break;
	                    } catch (Exception ex) {
	                    	if (i < 2) {
	                    		System.out.println("Retrying");
	                    		System.out.println(QueryFactory.create(subjectSparql, Syntax.syntaxARQ));
	                    		Thread.sleep(5000);
	                    		ex.printStackTrace();
	                    	} else {
	                    		throw ex;
	                    	}
	                    }
            		}
            		
            		client.bulk(br.build());
                }
	                
	            im.completeCurrentElement();
							
            } catch (Exception ex) {
//				logger.info("Indexing failed -- id: " + mc.idsToString());

				im.failCurrentElement(ex);
				
				throw ex;
			}
		}
	}	
	

//	public void unindex(Dataset dataset, ElasticConfiguration ec, String graph, IndexStructure indexStructure) throws IOException {
//		
//		try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
//		   
//			String id = getLocalIdentifier(database, dataset, indexStructure);
//
//			DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(id);
//			deleteRequest.setQuery(new TermQueryBuilder("graph", graph));
//			
//			BulkByScrollResponse bulkResponse = client.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);
//			
//			Response resp = client.getLowLevelClient().performRequest(new Request("GET", id + "/_stats"));
//			JsonNode body =  new ObjectMapper().readTree(resp.getEntity().getContent());
//			int size = body.get("indices").get(id).get("primaries").get("docs").get("count").asInt();
//
//			if (size == 0) {
//				DeleteIndexRequest request = new DeleteIndexRequest(id);
//				AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
//			}
//			
//		}
//	}	

	public List<Resource> skosHierarchy(String resource, ExpandIndexKeyMetadata expand)  {

		Element e = skosCache.get(resource);
		if (e != null) {
			return (List<Resource>)e.getObjectValue();
		}
		
//		System.out.println(">>>RES " + resource);

		List<Resource> res = new ArrayList<>();
		
		boolean exactMatch = expand.getMode().contains(ExpandType.SKOS_EXACT_MATCH);
		
		try {
			if (!(resource.startsWith("http://") || resource.startsWith("https://"))) {
				return res;
			}
		
//			URIDescriptor prefix = prefixesTrie.findPrefix(resource);
//			System.out.println(">>>PRE " + prefix);

//			if (prefix == null) {
//				return res;
//			}
//			VocabularyInfo vi = vocs.getMap().get(prefix.getPrefix());
//			
			List<ResourceContext> vi = expand.getVocabularContainer().resolve(resource);

//			System.out.println(">>>RCX " + vi);

			if (vi == null) {
				return res;
			}
			
			for (ResourceContext rc : vi) {
				DatasetContainer dc = ((DatasetContext)rc).getDatasetContainer();
				Dataset dataset = dc.getObject();
				
				String endpoint = null;
				String fromClause = null;
	
				if (!dataset.isRemote()) {
					DatasetCatalog dcg = schemaService.asCatalog(dataset);
					fromClause = schemaService.buildFromClause(dcg);
					endpoint = dc.getDatasetTripleStoreVirtuosoConfiguration().getSparqlEndpoint();
				} else {
					RemoteTripleStore rts = dataset.getRemoteTripleStore();
					fromClause = RemoteTripleStore.buildFromClause(rts);
					endpoint = rts.getSparqlEndpoint();
				}
				
				String sparql =  
						"CONSTRUCT { <" + resource + "> <http://local.in> ?broader ; <http://local.in> ?exact } " +
					    fromClause + 
					    "WHERE { " +
						"  OPTIONAL { <" + resource + "> <" + SKOSVocabulary.broader + ">+ ?broader . } " +
						   (exactMatch ? " OPTIONAL { <" + resource + "> <" + SKOSVocabulary.exactMatch + ">|(<" + SKOSVocabulary.broader + ">+)/<" + SKOSVocabulary.exactMatch + "> ?exact . }  " : "") +
						"}  ";
	
				
	//			System.out.println(">>>X " + sparql);
//				System.out.println(">>>X " + endpoint);
				
				try (QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, QueryFactory.create(sparql, Syntax.syntaxSPARQL_11))) {
					Model rs = qe.execConstruct();
					
					StmtIterator stmtIter =  rs.listStatements();
					
					while (stmtIter.hasNext()) {
						Statement stmt = stmtIter.next();
						
						res.add(stmt.getObject().asResource());
					}
					
				}
				
			}
			
//			System.out.println(">>>XXX " + res);
			
			skosCache.put(new Element(resource,res));
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return res;
	}	
	
	private class GeoPoint {
		public String lon;
		public String lat;
	}

   	private ByteArrayOutputStream  buildItem(String graph, Resource subject, Model model, SPARQLStructure ss, List<String> keys, Map<Integer, IndexKeyMetadata> keyMap, Map<String, ExpandIndexKeyMetadata> expandTypes) throws IOException {

		ByteArrayOutputStream jsonbos = new ByteArrayOutputStream();
		JsonFactory jsonFactory = new JsonFactory();				
		JsonGenerator jsonGenerator = jsonFactory.createGenerator(jsonbos, JsonEncoding.UTF8);
		
		jsonGenerator.writeStartObject();
		jsonGenerator.writeStringField("ctype", "dataset");
		jsonGenerator.writeStringField("iri", subject.getURI());
//		jsonGenerator.writeStringField("graph", graph);
		
//		System.out.println(">>> " + subject.getURI());
		
    	List<Set<String>> lexicalForms = new ArrayList<>();
    	List<Set<String>> languages = new ArrayList<>();
    	List<List<double[]>> vectors = new ArrayList<>();
    	
    	Map<String, Integer> keysToIndexMap = new HashMap<>();
    	
    	for (int i = 0; i < keys.size(); i++) {
    		String key = keys.get(i);
    		keysToIndexMap.put(key, i);
    		
//    		System.out.println(key);
    		
    		List<RDFNode> res  = ss.getResultForKey(model, subject, key);
//    		System.out.println(res);
    		
			ExpandIndexKeyMetadata expand = expandTypes.get(key);
			if (expand != null && expand.getMode().contains(ExpandType.SKOS_BROADER)) {
				Set<RDFNode> extra = new HashSet<>();
				for (RDFNode v : res) {
					if (v.isResource()) {
						extra.addAll(skosHierarchy(v.toString(), expand));
					}
				}
				res.addAll(extra);
			}

//    		System.out.println(res);

        	Set<String> lexicalForm = new LinkedHashSet<>();
        	lexicalForms.add(lexicalForm);
        	
        	Set<String> language = new LinkedHashSet<>();
        	languages.add(language);
        	
//        	List<String> uri = new ArrayList<>();
//        	uris.add(uri);
		    	
        	List<double[]> vector = new ArrayList<>();
        	vectors.add(vector);
        	
        	for (RDFNode node : res) {
	    		if (node.isResource()) {
	    			lexicalForm.add(node.asResource().toString());
	    		} else if (node.isLiteral()) {
	    			Literal lit = node.asLiteral();
	    			
   	    			if (lit.getDatatypeURI().equals(VectorDatatype.theTypeURI) ) {
   	    	    		
	    				vector.add((double[])lit.getValue());
	    				
	    			} else { 
	    				lexicalForm.add(lit.getLexicalForm());
	    			
		    			String lang = lit.getLanguage();
		    			if (lang != null && lang.length() > 0) {
		    				language.add(lang);
		    			}
	    			}
	    		}
	    	}

    	}
    	
    	Map<String, Set<String>> mergedLexicalForms = new HashMap<>();
    	Map<String, Set<String>> mergedLanguages = new HashMap<>();
    	Map<String, List<double[]>> mergedVectors = new HashMap<>();
    	
    	Map<String, List<String>> fieldsToKeysMap = new HashMap<>();
    	
    	for (int i = 0; i < keys.size(); i++) {
    		String key = keys.get(i);
    	
    		int keyIndex = Integer.parseInt(key.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);
    		
    		String keyName = ikm == null ? key : ikm.getName();
    		
    		List<String> keysForName = fieldsToKeysMap.get(keyName);
    		
    		if (keysForName == null) {
    			keysForName = new ArrayList<>();
    			fieldsToKeysMap.put(keyName, keysForName);
    		}
    		keysForName.add(key);
    	}
    	
    	for (Map.Entry<String, List<String>> entry : fieldsToKeysMap.entrySet()) {
    		String fieldName = entry.getKey();
    		
        	Set<String> sLexicalForms = new HashSet<>();
        	Set<String> sLanguages = new HashSet<>();
        	List<double[]> sVectors = new ArrayList<>();
        	
        	mergedLexicalForms.put(fieldName, sLexicalForms);
        	mergedLanguages.put(fieldName, sLanguages);
        	mergedVectors.put(fieldName, sVectors);
    	
    		for (String key : entry.getValue()) {
    			int i = keysToIndexMap.get(key);
    			
    			sLexicalForms.addAll(lexicalForms.get(i));
    			sLanguages.addAll(languages.get(i));
    			sVectors.addAll(vectors.get(i));
    		}
    	}
    	
    	Map<String, GeoPoint> geoMap = new HashMap<>();

    	for (String keyName : mergedLexicalForms.keySet()) {
    		
    		String firstKey = fieldsToKeysMap.get(keyName).iterator().next();
    		
    		int keyIndex = Integer.parseInt(firstKey.substring(1));
    		IndexKeyMetadata ikm = keyMap.get(keyIndex);

    		Set<String> keyLexicalForms = mergedLexicalForms.get(keyName);
    		Set<String> keyLanguages = mergedLanguages.get(keyName);
    		List<double[]> keyVectors = mergedVectors.get(keyName);
    		
//    		String keyName = ikm == null ? key : ikm.getName();
    		
    		if (ikm.getDatatype().equals("geo_point")) {
    			if (keyLexicalForms.size() == 1) { // what if size > 1
					int p = keyName.lastIndexOf(".");
					if (p != -1) {
						String suf = keyName.substring(p + 1);
						if (suf.equals("lat") || suf.equals("lon")) {
							String geoKeyName = keyName.substring(0, p);
							GeoPoint point = geoMap.get(geoKeyName);
							if (point == null) {
								point = new GeoPoint();
								geoMap.put(geoKeyName, point);
							}
							
							if (suf.equals("lat")) {
								point.lat = keyLexicalForms.iterator().next().toString();
							} else if (suf.equals("lon")) {
								point.lon = keyLexicalForms.iterator().next().toString();
							}
						}
					}
	    		}
    			continue;
    		}
    		
			if (keyLexicalForms.size() > 1) {
				jsonGenerator.writeFieldName(keyName);
				jsonGenerator.writeStartArray();
		    	for (String lexicalForm : keyLexicalForms) {
		    		jsonGenerator.writeString(lexicalForm);
		    	}
		    	jsonGenerator.writeEndArray();
				
    		} else if (keyLexicalForms.size() == 1) {
    			jsonGenerator.writeStringField(keyName, keyLexicalForms.iterator().next());
    		}
			
			if (ikm.isLanguageField()) {
	    		if (keyLanguages.size() > 1) {
			    	jsonGenerator.writeFieldName(keyName + "-lang");
					jsonGenerator.writeStartArray();
			    	for (String language : keyLanguages) {
			    		jsonGenerator.writeString(language);
			    	}
			    	jsonGenerator.writeEndArray();
	    		} else if (keyLanguages.size() == 1) {
	    			jsonGenerator.writeStringField(keyName + "-lang", keyLanguages.iterator().next());
	    		}
			}
			
    		if (keyVectors.size() == 1) {
    			jsonGenerator.writeFieldName(keyName);
    			jsonGenerator.writeArray(keyVectors.get(0), 0, keyVectors.get(0).length);
    		}
    		
    	}
    	
    	for (Map.Entry<String, GeoPoint> entry : geoMap.entrySet()) {
    		GeoPoint value = entry.getValue();
    		if (value.lat != null && value.lon != null) {
    			jsonGenerator.writeStringField(entry.getKey(), value.lat + "," + value.lon);
    		}
    	}

//    	for (int i = 0; i < keys.size(); i++) {
//    		String key = keys.get(i);
//    		
//    		int keyIndex = Integer.parseInt(key.substring(1));
//    		IndexKeyMetadata ikm = keyMap.get(keyIndex);
//
//    		String keyName = ikm == null ? key : ikm.getName();
//    		
//    		if (ikm.getDatatype().equals("geo_point")) {
//    			if (lexicalForms.get(i).size() == 1) { // what if size > 1
//					int p = keyName.lastIndexOf(".");
//					if (p != -1) {
//						String suf = keyName.substring(p + 1);
//						if (suf.equals("lat") || suf.equals("lon")) {
//							String geoKeyName = keyName.substring(0, p);
//							GeoPoint point = geoMap.get(geoKeyName);
//							if (point == null) {
//								point = new GeoPoint();
//								geoMap.put(geoKeyName, point);
//							}
//							
//							if (suf.equals("lat")) {
//								point.lat = lexicalForms.get(i).iterator().next().toString();
//							} else if (suf.equals("lon")) {
//								point.lon = lexicalForms.get(i).iterator().next().toString();
//							}
//						}
//					}
//	    		}
//    			continue;
//    		}
//    		
//			if (lexicalForms.get(i).size() > 1) {
//				jsonGenerator.writeFieldName(keyName);
//				jsonGenerator.writeStartArray();
//		    	for (String lexicalForm : lexicalForms.get(i)) {
//		    		jsonGenerator.writeString(lexicalForm);
//		    	}
//		    	jsonGenerator.writeEndArray();
//				
//    		} else if (lexicalForms.get(i).size() == 1) {
//    			jsonGenerator.writeStringField(keyName, lexicalForms.get(i).iterator().next());
//    		}
//			
//			if (ikm.isLanguageField()) {
//	    		if (languages.get(i).size() > 1) {
//			    	jsonGenerator.writeFieldName(keyName + "-lang");
//					jsonGenerator.writeStartArray();
//			    	for (String language : languages.get(i)) {
//			    		jsonGenerator.writeString(language);
//			    	}
//			    	jsonGenerator.writeEndArray();
//	    		} else if (languages.get(i).size() == 1) {
//	    			jsonGenerator.writeStringField(keyName + "-lang", languages.get(i).iterator().next());
//	    		}
//			}
//			
//    		if (vectors.get(i).size() == 1) {
//    			jsonGenerator.writeFieldName(keyName);
//    			jsonGenerator.writeArray(vectors.get(i).get(0), 0, vectors.get(i).get(0).length);
//    		}
//    		
//    	}
//    	
//    	for (Map.Entry<String, GeoPoint> entry : geoMap.entrySet()) {
//    		GeoPoint value = entry.getValue();
//    		if (value.lat != null && value.lon != null) {
//    			jsonGenerator.writeStringField(entry.getKey(), value.lat + "," + value.lon);
//    		}
//    	}

    	jsonGenerator.writeEndObject();
    	jsonGenerator.flush();
    	
    	return jsonbos;
		
	}

	public static String getLocalIdentifier(Database database, Dataset dataset, IndexStructure indexStructure) {
		return database.getName() + "-" + dataset.getIdentifier() + "-" + indexStructure.getIdentifier();
	}
	
	public boolean existsIndex(Database database, Dataset dataset, ElasticConfiguration ec, IndexStructure indexStructure) throws Exception {
		
		ElasticsearchClient client = ec.getClient();
		String indexName = getLocalIdentifier(database, dataset, indexStructure);
		
		BooleanResponse exists = client.indices().exists(existsBuilder -> existsBuilder.index(indexName));
		
		return exists.value();
	}
    	
	public void deleteIndex(Database database, Dataset dataset, ElasticConfiguration ec, IndexStructure indexStructure) throws Exception {
		
		ElasticsearchClient client = ec.getClient();
		String indexName = getLocalIdentifier(database, dataset, indexStructure);

		BooleanResponse exists = client.indices().exists(existsBuilder -> existsBuilder.index(indexName));
			
		if (exists.value()) {
			client.indices().delete(deleteBuilder -> deleteBuilder.index(indexName));
		}
	}

	public boolean createIndex(Database database, Dataset dataset, ElasticConfiguration ec, IndexStructure idxStruct) throws Exception {
		
		ElasticsearchClient client = ec.getClient();
		String indexName = getLocalIdentifier(database, dataset, idxStruct);
		
		BooleanResponse exists = client.indices().exists(existsBuilder -> existsBuilder.index(indexName));
		
		if (!exists.value()) {

			String settings = "";
			String analyzer = "";
			String charFilter = "";

			Map<String, String> analyzers = new HashMap<>();
			Map<String, String> filters = new HashMap<>();
			for (IndexKeyMetadata ikm : idxStruct.getKeysMetadata()) {
				TokenAnalyzer ta = ikm.getAnalyzer();
				
				if (ta != null) {
					String ea = ta.toElasticAnalyzer();
					String v = analyzers.put(ta.buildName(), ea);
					if (v == null) {
						if (analyzers.size() > 1) {
							analyzer += ", ";
						}
						analyzer += ea;
					}
					
					for (Map.Entry<String,String> flt : ta.buildCharFilter().entrySet()) {
						v = filters.put(flt.getKey(), flt.getValue());
//						System.out.println(flt.getKey() + " " + v);
						if (v == null) {
							if (filters.size() > 1) {
								charFilter += ", ";
							}
							
							charFilter += flt.getValue();
						}
					}
				}
			}
			
//			System.out.println("AN "  + analyzers);
//			System.out.println("CF "  + charFilter);
			
			if (analyzers.size() > 0) {
				settings += " \"settings\": { " +
					        "    \"analysis\": { " +
				            "       \"analyzer\": { " + analyzer + " } ";
				if (charFilter.length() > 0) {
					settings += ",   \"char_filter\": { " + charFilter + " } ";
				}
				settings +=
				        "           " +
				        "       } " +
				        "    }, ";
			}
			
			
			String mappings = 
					" \"mappings\": { " +
				    "      \"properties\": { " +
				    "        \"ctype\": { " +
				    "          \"type\": \"keyword\" " +
				    "        }, " +
				    "        \"iri\": { " +
				    "          \"type\": \"keyword\" " +
				    "        }, " +
				    "        \"graph\": { " +
					"          \"type\": \"keyword\" " +
					"        } ";

			Set<String> geopointTypes = new HashSet<>();

			Set<String> usedNames = new HashSet<>();
			
			for (IndexKeyMetadata ikm : idxStruct.getKeysMetadata()) {
				String keyName = idxStruct.keyName(ikm);
				if (!usedNames.add(keyName)) {
					continue;
				}
				
	    		if (ikm.getDatatype() != null) {
	    			if (ikm.getDatatype().equals("dense_vector")) {
						mappings += ", " +
								"\"" + keyName + "\": { " +
								   "\"type\": \"dense_vector\", " +
								   "\"dims\": 1024, " +
								   "\"index\": true, " +
								   "\"similarity\": \"cosine\" " +
								"} ";
	    			} else if (ikm.getDatatype().equals("geo_point")) {
						int p = keyName.lastIndexOf(".");
						if (p != -1) {
							String suf = keyName.substring(p + 1);
							if (suf.equals("lat") || suf.equals("lon")) {
								String geoKeyName = keyName.substring(0, p);
								if (geopointTypes.add(geoKeyName)) {
					    			mappings += ", " +
											"    \"" + geoKeyName + "\": { " +
											"        \"type\": \"" + ikm.getDatatype() + "\" " +
											"    } ";	
								}
							} else {
								p = -1;
							}
						}
						
						if (p == -1) {
			    			mappings += ", " +
									"    \"" + keyName + "\": { " +
									"        \"type\": \"" + ikm.getDatatype() + "\" " +
									"    } ";	
						}
		    		} else {
		    			mappings += ", " +
							"    \"" + keyName + "\": { " +
							"    \"type\": \"" + ikm.getDatatype() + "\" " +
							(ikm.getDatatype().equals("text") ? (",  \"fielddata\": true ") : "") +
							(ikm.getAnalyzer() != null ? (", \"analyzer\": \"" + ikm.getAnalyzer().buildName() + "\"") : "") + 
							"    } " +
							(ikm.isLanguageField() ?
							",   \"" + keyName + "-lang\": { " +
							"        \"type\": \"keyword\" " +
							"    } " 
							: 
							"");
		    		}
	    		}
			}
			
			mappings +=
					"    } " +			    
					"} ";

				
//			System.out.println("{ " + settings + " " + mappings + " }");
			final StringReader jsonReader = new StringReader("{ " + settings + " " + mappings + " }");
			
			CreateIndexResponse createResponse = ec.getClient().indices()
				    .create(createIndexBuilder -> createIndexBuilder
				        .index(indexName)
				        .withJson(jsonReader)
				    );
			
			return createResponse.acknowledged();
		} else {
			return false;
		}
	}    	
	
}
