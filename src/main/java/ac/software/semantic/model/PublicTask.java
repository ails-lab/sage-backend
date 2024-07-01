package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.state.TaskState;
import ac.software.semantic.model.constants.type.PublicTaskType;
import java.util.Date;
import java.util.Map;


@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "PublicTasks")
public class PublicTask {

    @Id
    private ObjectId id;
    
    private ObjectId databaseId;
    
    private ObjectId datasetId;
    private String datasetIdentifier;
    
    private ObjectId fileSystemConfigurationId;
    
    private String uuid;
    
    private PublicTaskType type;
    private Map<String, Object> parameters;
    
    private String outputFile;
    
    private Date startedAt;
    private Date completedAt;
    
    private TaskState state;
    
    private AnnotationExportPublicTaskData annotationExportData;
    
    public PublicTask() {
    	
    }
    
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
	
	public PublicTaskType getType() {
		return type;
	}
	
	public void setType(PublicTaskType type) {
		this.type = type;
	}
	
	public Map<String, Object> getParameters() {
		return parameters;
	}
	
	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Date getStartedAt() {
		return startedAt;
	}

	public void setStartedAt(Date startedAt) {
		this.startedAt = startedAt;
	}

	public Date getCompletedAt() {
		return completedAt;
	}

	public void setCompletedAt(Date completedAt) {
		this.completedAt = completedAt;
	}

	public TaskState getState() {
		return state;
	}

	public void setState(TaskState state) {
		this.state = state;
	}

	public AnnotationExportPublicTaskData getAnnotationExportData() {
		return annotationExportData;
	}

	public void setAnnotationExportData(AnnotationExportPublicTaskData annotationExportData) {
		this.annotationExportData = annotationExportData;
	}

	public ObjectId getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(ObjectId datasetId) {
		this.datasetId = datasetId;
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	public String getDatasetIdentifier() {
		return datasetIdentifier;
	}

	public void setDatasetIdentifier(String datasetIdentifier) {
		this.datasetIdentifier = datasetIdentifier;
	}
    

}
