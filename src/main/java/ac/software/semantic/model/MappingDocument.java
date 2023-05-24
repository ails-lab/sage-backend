package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.MappingType;
import ac.software.semantic.model.state.MappingState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "MappingDocuments")
public class MappingDocument {
   
   @Id
   private ObjectId id;

   private ObjectId userId;
   private ObjectId datasetId;
   
   private ObjectId databaseId;
   
   private String name;
   private String d2rml; // legacy only json d2rml;
   private String fileName;
   private String fileContents;
   
   private String uuid;
   
   private MappingType type;
   
   private List<String> parameters;
   
   private List<DependencyBinding> dependencies;
   
   private List<MappingInstance> instances;
   
   private List<String> dataFiles;
   
   private Date updatedAt;
   
   private ObjectId templateId;

   public MappingDocument() {  
	   this.instances = new ArrayList<>();
	   this.dependencies = new ArrayList<>();
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
   
//	public ArrayList<FileMapping> getFiles() {
//		return files;
//	}
//	
//	public void setFiles(ArrayList<FileMapping> files) {
//		this.files = files;
//	}
	
	@Override
	public String toString() {
		return String.format("D2RMLEntry{id='%s', userId='%s', d2rml=%s}\n",
//    		   id.toString(), userId.toString(), d2rml);
				id.toString(), userId.toString());
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	
//	public void setVirtuosoDatasetId(String virtuosoDatasetId) {
//		this.virtuosoDatasetId = virtuosoDatasetId;
//	}

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
		return parameters;
	}
	
	public boolean hasParameters() {
		return parameters != null && parameters.size() > 0;
	}

	public void setParameters(List<String> parameters) {
		if (parameters == null) {
			parameters = new ArrayList<>();
		}
		
		if (this.parameters == null) {
		   this.parameters = parameters;
		   this.instances.clear(); // should delete first instances!!! what abut executions and files???
	       
	       if (parameters.isEmpty()) {
	    	   instances.add(new MappingInstance());
	       }
		} else { // what if parameters change???
			this.parameters = parameters;
		}
		
		this.parameters = parameters;
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

	public List<String> getDataFiles() {
		return dataFiles;
	}

	public void setDataFiles(List<String> dataFiles) {
		this.dataFiles = dataFiles;
	}
	
	public void addDataFile(String dataFile) {
		if (this.dataFiles == null) {
			this.dataFiles = new ArrayList<>();
		}
		
		if (!this.dataFiles.contains(dataFile)) {
			this.dataFiles.add(dataFile);
		}
	}
	
	
	public void removeDataFile(String dataFile) {
		if (dataFiles != null) {
			dataFiles.remove(dataFile);
			
			if (dataFiles.size() == 0) {
				dataFiles = null;
			}
		}
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}
	
	
	
}