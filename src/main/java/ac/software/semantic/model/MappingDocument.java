package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.ContentDocument;
import ac.software.semantic.model.base.DatasetContained;
import ac.software.semantic.model.base.GroupedDocument;
import ac.software.semantic.model.base.OrderedDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.constants.state.MappingState;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.model.constants.type.MappingType;
import ac.software.semantic.model.state.MappingExecuteState;
import ac.software.semantic.model.state.MappingPublishState;
import ac.software.semantic.service.exception.TaskConflictException;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "MappingDocuments")
public class MappingDocument implements DatasetContained, ContentDocument, OrderedDocument, GroupedDocument, DatedDocument, IdentifiableDocument {
   
   @Id
   private ObjectId id;

   @JsonIgnore
   private ObjectId databaseId;
   
   private ObjectId userId;
   
   @Indexed
   private ObjectId datasetId;
   
   private String datasetUuid;
   
   private String name;
   private String d2rml; // legacy only json d2rml;
   private String fileName;
   private String fileContents;
   
   private String uuid;
   
   private String identifier;
   
   private String description;
   
   private List<String> groupIdentifiers;
   
   private MappingType type;
   
   private List<String> parameters;
   private List<DataServiceParameter> parametersMetadata;
   
   private List<DependencyBinding> dependencies;
   
   private List<MappingInstance> instances;
   
   private List<MappingDataFile> dataFiles;
   
   private Date createdAt;
   private Date updatedAt;
   
   private ObjectId templateId;
   
   private List<ObjectId> shaclId;
   private ObjectId d2rmlId;
   private Boolean d2rmlIdBound;
   
   private boolean active;
   
   private int order;
   private int group;

   private MappingDocument() {  
	   this.instances = new ArrayList<>();
//	   this.dependencies = new ArrayList<>();
   }
   
   public MappingDocument(Dataset dataset) {
	   this();
	   
	   this.databaseId = dataset.getDatabaseId();
	   
	   this.datasetId = dataset.getId();
	   this.datasetUuid = dataset.getUuid();
   }
   
   public ObjectId getId() {
       return id;
   }
   
   public String getD2RML() {
       return d2rml;
   }

   public void setD2RML(String d2rml) {
       this.d2rml = d2rml;
   }

   public String getName() {
       return name;
   }
   
   public void setName(String name) {
       this.name = name;
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

	public MappingType getType() {
		return type;
	}

	public void setType(MappingType type) {
		this.type = type;
	}
	
	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public List<String> getParameters() {
		if (parameters != null && parameters.size() == 0) {
			return null;
		} else {
			return parameters;
		}
	}
	
	private boolean areParametersSame(List<String> oldParameters, List<String> newParameters) {
		if (oldParameters == null && newParameters == null) {
			return true;
		}
		
		if ((oldParameters == null && newParameters != null) || (oldParameters != null && newParameters == null)) {
			return false;
		}
		
		if (oldParameters.size() != newParameters.size()) {
			return false;
		}
		
		for (String s : oldParameters) {
			if (!newParameters.contains(s)) {
				return false;
			}
		}
		
		return true;
	}
	
	public boolean hasParameters() {
		return parameters != null && parameters.size() > 0;
	}

	private static List<String> getParameterNames(List<DataServiceParameter> parameters) {
		if (parameters != null) {
			List<String> res = new ArrayList<>();
			for (DataServiceParameter dsp : parameters) {
				res.add(dsp.getName());
			}
			return res;
		}
		
		return null;
	}
	
	public void applyParameters(List<DataServiceParameter> parameters) throws Exception {
		if (parameters != null && parameters.size() == 0) {
			parameters = null;
		}
		
		List<String> oldParameters = getParameters();
		List<String> newParameters = getParameterNames(parameters);
		
		if (oldParameters != null && oldParameters.size() == 0) {
			oldParameters = null;	
		}
		
		if (!areParametersSame(oldParameters, newParameters)) {
			if (getInstances() != null) {
				for (MappingInstance mi : getInstances()) {
					if (mi.getExecute() != null) {
						for (MappingExecuteState es : mi.getExecute()) {
							if (es.getExecuteState() != MappingState.NOT_EXECUTED) {
								throw new Exception("Parameters can be changed only for not executed mappings.");
							}
						}
					}
					if (mi.getPublish() != null) {
						for (MappingPublishState ps : mi.getPublish()) {
							if (ps.getPublishState() != DatasetState.UNPUBLISHED) {
								throw new Exception("Parameters can be changed only for unpublished mappings.");
							}
						}
					}
				}
				
				this.instances.clear();  //should delete also dataFiles !!!!
			}
		}

		this.parameters = newParameters;
		this.parametersMetadata = parameters;
		
		if (parameters == null && instances.isEmpty()) {
	       instances.add(new MappingInstance());
		}
	}

	public List<MappingInstance> getInstances() {
		return instances;
	}
	
	public MappingInstance getInstance(ObjectId id) {
		if (instances != null) {
			for (MappingInstance mi : instances) {
				if (mi.getId().equals(id)) {
					return mi;
				}
			}
		}
		
		return null;
	}
	
	
	public void setInstances(List<MappingInstance> instances) {
		this.instances = instances;
	}
	
//	public void addInstance(MappingInstance mi) {
//		this.instances.add(mi);
//	}
	
	public MappingInstance addInstance(List<ParameterBinding> bindings) {
		MappingInstance mi = new MappingInstance();
		mi.setBinding(bindings);
		this.instances.add(mi);
		
		return mi;
	}


//	public void setType(Type type) {
//		this.type = type;
//	}
	
	public boolean isExecuted(ObjectId fileSystemConfigurationId) {
		for (MappingInstance mi : getInstances()) {
			if (mi.getExecuteState(fileSystemConfigurationId).getExecuteState() != MappingState.EXECUTED) {
				return false;
			}
		}
		
		return true;
	}

	public List<DependencyBinding> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<DependencyBinding> dependencies) {
		this.dependencies = dependencies;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public String getFileContents() {
		return fileContents;
	}

	public void setFileContents(String fileContents) {
		this.fileContents = fileContents;
	}

	public ObjectId getTemplateId() {
		return templateId;
	}

	public void setTemplateId(ObjectId templateId) {
		this.templateId = templateId;
	}

	public List<MappingDataFile> getDataFiles() {
		return dataFiles;
	}

	public void setDataFiles(List<MappingDataFile> dataFiles) {
		this.dataFiles = dataFiles;
	}
	
	public void addDataFile(MappingDataFile dataFile) throws TaskConflictException {
		if (this.dataFiles == null) {
			this.dataFiles = new ArrayList<>();
		}

		for (int i = 0; i < dataFiles.size();i++) {
			MappingDataFile mdf = dataFiles.get(i);
			if (mdf.getFilename().equals(dataFile.getFilename()) && mdf.getFileSystemConfigurationId().equals(dataFile.getFileSystemConfigurationId())) {
				throw new TaskConflictException("An attachment with the same name already exists.");
			}
		}

		this.dataFiles.add(dataFile);
	}
	
	
	public void removeDataFile(MappingDataFile dataFile) {
		if (dataFiles != null) {
			for (int i = 0; i < dataFiles.size(); i++) {
				MappingDataFile mdf = dataFiles.get(i);
				if (mdf.getFilename().equals(dataFile.getFilename()) && mdf.getFileSystemConfigurationId().equals(dataFile.getFileSystemConfigurationId())) {
					dataFiles.remove(i);
					break;
				}
			}
			
			if (dataFiles.size() == 0) {
				dataFiles = null;
			}
		}
	}
	
	public List<MappingDataFile> checkDataFiles(FileSystemConfiguration fileSystemConfiguration) {
		
		List<MappingDataFile> res = new ArrayList<>();
		if (dataFiles != null) {		
			for (MappingDataFile s : dataFiles) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfiguration.getId())) {
					res.add(s);
				}
			}
		}
		
		if (res.size() > 0) {
			return res;
		} else {
			return null;
		}
	}	

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public String getDatasetUuid() {
		return datasetUuid;
	}

	public void setDatasetUuid(String datasetUuid) {
		this.datasetUuid = datasetUuid;
	}

	public List<ObjectId> getShaclId() {
		return shaclId;
	}

	public void setShaclId(List<ObjectId> shaclId) {
		this.shaclId = shaclId;
	}

	@Override
	public String getContent() {
		return getFileContents();
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	@Override
	public int getOrder() {
		return order;
	}

	@Override
	public void setOrder(int order) {
		this.order = order;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public ObjectId getD2rmlId() {
		return d2rmlId;
	}

	public void setD2rmlId(ObjectId d2rmlId) {
		this.d2rmlId = d2rmlId;
	}

	public Boolean getD2rmlIdBound() {
		return d2rmlIdBound;
	}

	public void setD2rmlIdBound(Boolean d2rmlIdBound) {
		this.d2rmlIdBound = d2rmlIdBound;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public List<String> getGroupIdentifiers() {
		return groupIdentifiers;
	}

	public void setGroupIdentifiers(List<String> groupIdentifiers) {
		this.groupIdentifiers = groupIdentifiers;
	}

	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	public void clearInstances() {
		instances = new ArrayList<>();
	}

	public List<DataServiceParameter> getParametersMetadata() {
		return parametersMetadata;
	}

	public void setParametersMetadata(List<DataServiceParameter> parametersMetadata) {
		this.parametersMetadata = parametersMetadata;
	}
}
