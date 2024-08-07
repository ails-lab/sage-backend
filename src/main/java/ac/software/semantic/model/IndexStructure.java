package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.types.ObjectId;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.index.ClassIndexElement;
import ac.software.semantic.model.index.IndexKeyMetadata;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "IndexStructures")
public class IndexStructure implements SpecificationDocument, DatedDocument, IdentifiableDocument {

	@Id
	private ObjectId id;

	@JsonIgnore
	private ObjectId databaseId;
	
	private String name;
	
//	private ObjectId elasticConfigurationId;
	
	private ObjectId userId;
	
	private ObjectId datasetId;
	private String datasetUuid;
	
	private String uuid;
	private String identifier;
	
//	@Transient
//	private ElasticConfiguration ec;
	
//	private TreeSet<String> keys;
//	private String labelKey;
	
	private Date createdAt;
	private Date updatedAt;

//	private int counter = 0;

	private List<ClassIndexElement> elements;
	
	private List<IndexKeyMetadata> keysMetadata;
	
	private String description;
	
	private ObjectId schemaDatasetId;
	
	private Object schema;

//	@Transient
//	Counter cc;
	
//	@Autowired
//	@Transient
//	@JsonIgnore
//	private DatasetRepository datasetsRepository;
//	
	
//	private Map<String, List<PathElement>> keyMap;
	
	private IndexStructure() {
		super();
	}
	
	public IndexStructure(Database database) {
		this();
		
		this.databaseId = database.getId();
	}
	
	public IndexStructure(Dataset dataset) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = dataset.getDatabaseId();
		   
		this.datasetId = dataset.getId();
		this.datasetUuid = dataset.getUuid();
	}
	
//	public IndexStructure() {
////		elements = new ArrayList<>();
////		keys = new TreeSet<>();
////		keyMap = new HashMap<>();
//		
////		cc = new Counter(0);
//	}

//	public ObjectId getElasticConfigurationId() {
//		return elasticConfigurationId;
//	}
//
//
//	public void setElasticConfigurationId(ObjectId elasticId) {
//		this.elasticConfigurationId = elasticId;
//	}


//	public String getName() {
//		return name;
//	}
//
//
//	public void setName(String name) {
//		this.name = name;
//	}


	public String getIdentifier() {
		return identifier;
	}


	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	
//	public Map<String, List<PathElement>> getKeyMap() {
//		return keyMap;
//	}

//	public Set<String> getKeys() {
//		return keyMap.keySet();
//	}
	
//	private String lookupPathKey(List<Resource> list) {
//		
//		for (Map.Entry<String, List<PathElement>> entry : keyMap.entrySet()) {
//			if (equals(entry.getValue(), list)) {
//				return entry.getKey();
//			}
//		}
//		
//		return null;
//	}
	
//	public String getPathKey(List<Resource> list) {
//		
//		String r = lookupPathKey(list);
//		if (r != null) {
//			return r;
//		} else {
//			return addPathKey(list); 
//		}
//	}
	
//	private String addPathKey(List<Resource> list) {
//		
//		List<PathElement> path = new ArrayList<>();
//		for (int i = 0; i < list.size(); i++) {
//			Resource r = list.get(i);
//			
//			if (r instanceof Property) {
//				path.add(PathElement.createPropertyPathElement(r.getURI()));
//			} else { 
//				path.add(PathElement.createClassPathElement(r.getURI()));				
//			}
//		}
//		
//		String key = "r" + keyMap.size();
//		keyMap.put(key, path);
//		updateIndex(key);
//		
//		return key;
//	}
//	
	
//	private static boolean equals(List<PathElement> path, List<Resource> list) {
//		
//		if (path.size() != list.size()) {
//			return false;
//		}
//		
//		for (int i = 0; i < path.size(); i++) {
//			PathElement p = path.get(i);
//			Resource r = list.get(i);
//			
//			if (p.getUri().equals(r.getURI())) {
//				if (p.getType().equals("property") && r instanceof Property || p.getType().equals("class") && !(r instanceof Property)) {
//					continue;
//				} else {
//					return false;
//				}
//			} else {
//				return false;
//			}
//		}
//		
//		return true;
//	}
	
	public String keyName(IndexKeyMetadata ikm) {
		if (ikm.getName() != null) {
			return ikm.getName(); 
		} else {
			return "r" + ikm.getIndex();
		}
	}


	
	
	
//	private void updateIndex(Database database, String key) {
//	
//		try(RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(ec.getIndexIp(), ec.getIndexPort(), "http")))) {
//		
//			GetIndexRequest getRequest = new GetIndexRequest(getLocalIdentifier(database)); 
//				
////			logger.info("Data index: " + client.indices().exists(getRequest, RequestOptions.DEFAULT));
//			
//			if (client.indices().exists(getRequest, RequestOptions.DEFAULT)) {
//			
//				PutMappingRequest request = new PutMappingRequest(getLocalIdentifier(database));
//
//				String json = 
//				" { " +
//			    "      \"properties\": { " +
//			    "        \"" + key + "-lexical-form\": { " +
////			    "          \"type\": \"text\", " +
////				"          \"fields\": { " +
////				"            \"raw\": { " + 
////				"              \"type\": \"keyword\" " +
////				"            } " +
////				"          } " +
//				"          \"type\": \"search_as_you_type\" " +
//			    "        }, " +
////			    "        \"" + key + "-lexical-form-raw\": { " +
////			    "          \"type\": \"text\", " +
////				"          \"fields\": { " +
////				"            \"raw\": { " + 
////				"              \"type\": \"keyword\" " +
////				"            } " +
////				"          } " +
////			    "        }, " +
//			    "        \"" + key + "-lang\": { " +
//			    "          \"type\": \"keyword\" " +
//			    "        }, " +
//			    "        \"" + key + "-uri\": { " +
//			    "          \"type\": \"keyword\" " +
//			    "        } " +					    
//				"    } " +			    
//			    "} ";
//				
////			    System.out.println(json);
//
//				  request.source(json,	XContentType.JSON);
//				
//				client.indices().putMapping(request, RequestOptions.DEFAULT);
//			}		
//
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//	}
	
//	public ElasticConfiguration bind(ConfigurationContainer<ElasticConfiguration> elasticConfigurations) {
//		ec = elasticConfigurations.getById(elasticConfigurationId);
//		
//		return ec;
//	}

//	public void prepareStructure(List<IndexElement> ies) {
//		for (IndexElement ie : ies) {
//			SPARQLStructure ss = ie.toSPARQL();
//			
//			List<List<Resource>> paths = ss.getPaths();
//			for (List<Resource> list : paths) {
//				getPathKey(list);
//			}
//		}
//	}

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
	
//	private void keysToIndexElement() {
//		List<IndexElement> res = new ArrayList<>();
//	
//		Model iemodel = ModelFactory.createDefaultModel();
//
//		IndexElement lastElement = null;
//
//		for (int i = 0; i < keyMap.size(); i++) {
//			List<PathElement> list = keyMap.get("r" + 1);
//
//			IndexElement previousElement = null;
//			Property previousProperty = null;
//
//			int j = 0;
//			if (lastElement != null) {
//				for (; j < list.size(); j++) {
//					PathElement ce = list.get(j);
//					if (ce.isClass() && lastElement.getClazz().toString().equals(ce.getUri())) {
//						previousElement = lastElement;
//					} else if (ce.isProperty()) {
//						IndexElement propElement = lastElement.getByProperty(iemodel.createProperty(ce.getUri()));
//						if (propElement != null) {
//							previousElement = propElement;
//						} else {
//							
//						}
//					} else {
//						break;
//					}
//				}
//			}
//			
//			for (; j < list.size(); j++) {
//				PathElement pe = list.get(j);
//				
//				if (lastElement != null) {
//					
//				} else {
//					if (pe.isClass()) {
//						IndexElement ie = new IndexElement(iemodel.createResource(pe.getUri()));
//							
//						if (j == 0) {
//							res.add(ie);
//							lastElement = ie;
//						} else {
//							previousElement.addProperty(previousProperty, ie);
//						}
//						
//						previousElement = ie;
//						previousProperty = null;
//					} else {
//						previousProperty = iemodel.createProperty(pe.getUri());
//					}
//				}
//			}
//
//			if (previousProperty != null) {
//				previousElement.addProperty(previousProperty);
//			}
//			
//		}
//	}

	public List<ClassIndexElement> getElements() {
		return elements;
	}

	public void setElements(List<ClassIndexElement> elements) {
//		this.counter = 0;
		this.elements = elements;
//		for (IndexElement ie : this.elements) {
//			readCounter(ie);
//		}
//		cc = new Counter(counter);
	}
//	
//	public void addElements(List<IndexElement> elements) {
//		this.elements.addAll(elements);
//		for (IndexElement ie : this.elements) {
//			setCounter(ie);
//		}
//		cc = new Counter(counter);
//	}
	
//	private void setCounter(IndexElement ie) {
//		for (NextIndexElement nie : ie.getProperties()) {
//			if (nie.getElement() == null) {
//				nie.setIndex(cc.increaseUse());
//				keys.add("r" + nie.getIndex());
//				counter = Math.max(counter, nie.getIndex());
//			} else {
//				setCounter(nie.getElement());
//			}
//		}
//	}
//	
//	private void readCounter(IndexElement ie) {
//		for (NextIndexElement nie : ie.getProperties()) {
//			if (nie.getElement() == null) {
//				keys.add("r" + nie.getIndex());
//				counter = Math.max(counter, nie.getIndex());
//			} else {
//				readCounter(nie.getElement());
//			}
//		}
//	}
	
	public List<String> getKeys() {
		List<String> res = new ArrayList<>();
		if (keysMetadata != null) {
			for (IndexKeyMetadata ikm : keysMetadata) {
				res.add("r" + ikm.getIndex());
			}
		}
		
		return res;
	}

//	public int getCounter() {
////		return counter;
//		return cc.getValue();
//	}
//
//	public void setCounter(int counter) {
//		this.counter = counter;
//	}

//	public String getLabelKey() {
//		return labelKey;
//	}
//
//	public void setLabelKey(String labelKey) {
//		this.labelKey = labelKey;
//	}

	public Map<Integer, IndexKeyMetadata> getKeyMetadataMap() {
		Map<Integer, IndexKeyMetadata> res = new HashMap<>();
		
		if (keysMetadata != null) {
			for (IndexKeyMetadata ikm : keysMetadata) {
				res.put(ikm.getIndex(), ikm);
			}
		}
		
		return res;
	}
	
	public void setKeysMetadata(List<IndexKeyMetadata> ikm) {
		this.keysMetadata = ikm;
	}

	public List<IndexKeyMetadata> getKeysMetadata() {
		return this.keysMetadata;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public ObjectId getSchemaDatasetId() {
		return schemaDatasetId;
	}

	public void setSchemaDatasetId(ObjectId schemaDatasetId) {
		this.schemaDatasetId = schemaDatasetId;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getSchema() {
		return schema;
	}

	public void setSchema(Object schema) {
		this.schema = schema;
	}
	
//	public ObjectId getDatasetId() {
//		return datasetId;
//	}
//
//	public void setDatasetId(ObjectId datasetId) {
//		this.datasetId = datasetId;
//	}
	
//	SPARQLStructure ss = ie0.toSPARQL();

//	public ElasticConfiguration getElasticConfiguration() {
//		return ec;
//	}
	
	
}
