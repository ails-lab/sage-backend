package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.InverseMemberDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Campaigns")
public class Campaign implements SpecificationDocument, MemberDocument, InverseMemberDocument<ProjectDocument>, DatedDocument {

	@Id
	private ObjectId id;

	@JsonIgnore
	private ObjectId databaseId;

	private String uuid;

	private ObjectId userId;

	private CampaignType type;

	private String name;

	private List<ObjectId> validatorId;

	private CampaignState state;

	private Date createdAt;
	private Date updatedAt;

	private List<ObjectId> datasetId;
	
	private List<ObjectId> projectId;
	
	public Campaign() {
		super();
	}
	
	public Campaign(Database database) {
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = database.getId();
	}

	public ObjectId getId() {
		return id;
	}

	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public CampaignType getType() {
		return type;
	}

	public void setType(CampaignType type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<ObjectId> getValidatorId() {
		return validatorId;
	}

	public void setValidatorId(List<ObjectId> validatorId) {
		this.validatorId = validatorId;
	}
	
	@Override
	public boolean hasMember(DataDocument member) {
		if (member instanceof User) {
			return MemberDocument.hasMemberId(member.getId(), validatorId);
		} else if (member instanceof Dataset) {
			return MemberDocument.hasMemberId(member.getId(), datasetId);
		} 
		
		return false;
	}

	@Override
	public void addMember(DataDocument member) {
		if (member instanceof User) {
			setValidatorId(MemberDocument.addMemberId(member.getId(), validatorId).list);
		} else if (member instanceof Dataset) {
			setDatasetId(MemberDocument.addMemberId(member.getId(), datasetId).list);
		}
	}

	@Override
	public void removeMember(DataDocument member) {
		if (member instanceof User) {
			setValidatorId(MemberDocument.removeMemberId(member.getId(), validatorId).list);
		} else if (member instanceof Dataset) {
			setDatasetId(MemberDocument.removeMemberId(member.getId(), datasetId).list);
		}
	}
	
	@Override
	public List<ObjectId> getMemberIds(Class clazz) {
		if (clazz == User.class) {
			return getValidatorId();
		} else if (clazz == Dataset.class) {
			return getDatasetId();
		}
		
		return null;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public CampaignState getState() {
		return state;
	}

	public void setState(CampaignState state) {
		this.state = state;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public List<ObjectId> getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(List<ObjectId> datasetId) {
		this.datasetId = datasetId;
	}

	public List<ObjectId> getProjectId() {
		return projectId;
	}

	public void setProjectId(List<ObjectId> projectId) {
		this.projectId = projectId;
	}

	@Override
	public void addTo(ProjectDocument source) {
		if (projectId == null) {
			projectId = new ArrayList<>();
		}
		projectId.add(source.getId());
		
	}

	@Override
	public void removeFrom(ProjectDocument source) {
		if (projectId != null) {
			projectId.remove(source.getId());
			
			if (projectId.isEmpty()) {
				projectId = null;
			}
		}
	}

	@Override
	public boolean isMemberOf(ProjectDocument target) {
		if (projectId != null) {
			return projectId.contains(target.getId());
		} else {
			return false;
		}
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}


}