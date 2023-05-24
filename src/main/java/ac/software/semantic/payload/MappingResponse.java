package ac.software.semantic.payload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import ac.software.semantic.model.MappingInstance;
import ac.software.semantic.model.state.MappingState;

public class MappingResponse implements Response {
    private String id;
    private String name;
//    private String d2rml;
    private String fileName;
//    private String fileContents;
    
    private String datasetId;
    private String uuid;
    private String type;
    
    private boolean template;

    private List<String> parameters;
    
    private List<MappingInstanceResponse> instances;
    
    private List<String> dataFiles;
    
//    private MappingState state;
//    
//    private Date executeStartedAt;
//    private Date executeCompletedAt;
    
    private List<String> files;

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

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(List<String> parameters) {
		this.parameters = parameters;
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

	
}
