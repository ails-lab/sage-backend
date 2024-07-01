package ac.software.semantic.payload.response;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.DataServiceParameter;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MappingResponse implements Response, MultiUserResponse {
    private String id;
    private String name;
//    private String d2rml;
    private String fileName;
//    private String fileContents;
    
    private String datasetId;
    private String uuid;
    private String type;
    
    private String description;
    
    private String identifier;
    private List<String> groupIdentifiers;
    
    private boolean template; // this is for frontend to show full mapping menu or not. do not remove/ 
    
    private String templateId;
    private String d2rmlId;
    private Boolean d2rmlIdBound;
    private String d2rmlName;

    private List<String> shaclId;

    private List<DataServiceParameter> parameters;
    
    private List<MappingInstanceResponse> instances;
    
    private List<String> dataFiles;
    
    private List<String> files;
    
    private boolean active;
    
    private int order;
    
    private Date createdAt;
    private Date updatedAt;
    
    private int group;
    
    private boolean ownedByUser;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

//    public String getD2RML() {
//        return d2rml;
//    }
//
//    public void setD2RML(String d2rml) {
//        this.d2rml = d2rml;
//    }

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(ArrayList<String> files) {
		this.files = files;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	
//	public Date getExecuteStartedAt() {
//		return executeStartedAt;
//	}
//
//	public void setExecuteStartedAt(Date startedAt) {
//		this.executeStartedAt = startedAt;
//	}
//
//	public Date getExecuteCompletedAt() {
//		return executeCompletedAt;
//	}
//
//	public void setExecuteCompletedAt(Date completedAt) {
//		this.executeCompletedAt = completedAt;
//	}
//
//	public MappingState getState() {
//		return state;
//	}
//
//	public void setState(MappingState state) {
//		this.state = state;
//	}

	public List<DataServiceParameter> getParameters() {
		return parameters;
	}

//	public void setParameters(List<DataServiceParameter> parameters) {
//		this.parameters = parameters;
//	}

	public void applyParameters(List<String> parameters, List<DataServiceParameter> parametersMetadata) {
		if (parametersMetadata != null) {
			this.parameters = parametersMetadata;
		} else {
			if (parameters != null) {
				parametersMetadata = new ArrayList<>();
				for (String p : parameters) {
					String name = p;
					boolean required = true; 
					if (p.endsWith("*")) {
						required = false;
						name = p.substring(0, p.length() - 1);
					}
					
					DataServiceParameter dsp = new DataServiceParameter(name);
					dsp.setRequired(required);
					
					parametersMetadata.add(dsp);
				}
			}
			this.parameters = parametersMetadata;
		}
	}
	
	public List<MappingInstanceResponse> getInstances() {
		return instances;
	}

	public void setInstances(List<MappingInstanceResponse> instances) {
		this.instances = instances;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
//
//	public String getFileContents() {
//		return fileContents;
//	}
//
//	public void setFileContents(String fileContents) {
//		this.fileContents = fileContents;
//	}

	public boolean isTemplate() {
		return template;
	}

	public void setTemplate(boolean template) {
		this.template = template;
	}

	public List<String> getDataFiles() {
		return dataFiles;
	}

	public void setDataFiles(List<String> dataFiles) {
		this.dataFiles = dataFiles;
	}

	public String getTemplateId() {
		return templateId;
	}

	public void setTemplateId(String templateId) {
		this.templateId = templateId;
	}

	public List<String> getShaclId() {
		return shaclId;
	}

	public void setShaclId(List<String> shaclId) {
		this.shaclId = shaclId;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public int getGroup() {
		return group;
	}

	public void setGroup(int group) {
		this.group = group;
	}

	public String getD2rmlId() {
		return d2rmlId;
	}

	public void setD2rmlId(String d2rmlId) {
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

	public String getD2rmlName() {
		return d2rmlName;
	}

	public void setD2rmlName(String d2rmlName) {
		this.d2rmlName = d2rmlName;
	}

	@Override
	public boolean isOwnedByUser() {
		return ownedByUser;
	}

	@Override
	public void setOwnedByUser(boolean ownedByUser) {
		this.ownedByUser = ownedByUser;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}	
}
