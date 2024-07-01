package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.RunnableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.RunningState;
import ac.software.semantic.model.constants.type.TaskType;
import ac.software.semantic.model.state.RunState;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "UserTasks")
public class UserTaskDocument implements SpecificationDocument, RunnableDocument, DatedDocument {
	
	@Id
	private ObjectId id;
	
	private ObjectId databaseId;
	
	private ObjectId fileSystemConfigurationId;
	
	private String uuid;
	
	@Indexed
	private ObjectId datasetId;
	
	private String datasetUuid;
	
	private Date createdAt;
	private Date updatedAt;

	private ObjectId userId;
	
	private String name;
	
	private String cronExpression;
	
	private boolean scheduled;
	
	private List<UserTaskDescription> tasks;
	
	private List<RunState> run;
	
	private boolean freshRunOnly;

	
   private UserTaskDocument() {   
   }

   public UserTaskDocument(Dataset dataset) {
	   this();
	
	   this.uuid = UUID.randomUUID().toString();
	   
	   this.databaseId = dataset.getDatabaseId();
	   
	   this.datasetId = dataset.getId();
	   this.datasetUuid = dataset.getUuid();
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

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
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

	public List<UserTaskDescription> getTasks() {
		return tasks;
	}

	public void setTasks(List<UserTaskDescription> tasks) {
		this.tasks = tasks;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public boolean isScheduled() {
		return scheduled;
	}

	public void setScheduled(boolean scheduled) {
		this.scheduled = scheduled;
	}

	@Override
	public List<RunState> getRun() {
		return run;
	}

	@Override
	public void setRun(List<RunState> run) {
		this.run = run;
	}

	@Override
	public RunState getRunState(ObjectId fileSystemConfigurationId) {
		if (run != null) {
			for (RunState s : run) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		} else {
			run = new ArrayList<>();
		}
		
		RunState s = new RunState();
		
		s.setRunState(RunningState.NOT_RUNNING);
		s.setFileSystemConfigurationId(fileSystemConfigurationId);
		
		run.add(s);
		
		return s;
	}

	@Override
	public RunState checkRunState(ObjectId fileSystemConfigurationId) {
		if (run != null) {
			for (RunState s : run) {
				if (s.getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					return s;
				}
			}
		}
		
		return null;
	}

	@Override
	public void deleteRunState(ObjectId fileSystemConfigurationId) {
		if (run != null) {
			for (int i = 0; i < run.size(); i++) {
				if (run.get(i).getFileSystemConfigurationId().equals(fileSystemConfigurationId)) {
					run.remove(i);
					break;
				}
			}
			
			if (run.size() == 0) {
				run = null;
			}
		}
	}

	public ObjectId getFileSystemConfigurationId() {
		return fileSystemConfigurationId;
	}

	public void setFileSystemConfigurationId(ObjectId fileSystemConfigurationId) {
		this.fileSystemConfigurationId = fileSystemConfigurationId;
	}

	public boolean isFreshRunOnly() {
		return freshRunOnly;
	}

	public void setFreshRunOnly(boolean freshRunOnly) {
		this.freshRunOnly = freshRunOnly;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	
}
