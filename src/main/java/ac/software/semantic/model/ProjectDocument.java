package ac.software.semantic.model;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.MemberDocument;
import ac.software.semantic.model.constants.type.IdentifierType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Projects")
public class ProjectDocument implements EnclosingDocument, MemberDocument<User>, DatedDocument, IdentifiableDocument {
	@Id
	private ObjectId id;

	private ObjectId userId;
	
	private String uuid;
	
	private ObjectId databaseId;

	private String name;
	
	private String identifier;

	@Field("public")
	private boolean publik;
	
	private Date createdAt;
	private Date updatedAt;
	
	private List<ObjectId> joinedUserId;
	
	private Boolean databaseDefault;

	private ProjectDocument() {
	}

	public ProjectDocument(Database database) {
		this();
		
		this.uuid = UUID.randomUUID().toString();
		
		this.databaseId = database.getId();

	}

	public ObjectId getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	@Override
	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public boolean isPublik() {
		return publik;
	}

	public void setPublik(boolean publik) {
		this.publik = publik;
	}

	public Date getUpdatedAt() {
		return updatedAt;
	}

	public void setUpdatedAt(Date updatedAt) {
		this.updatedAt = updatedAt;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	@Override
	public boolean hasMember(User member) {
		return MemberDocument.hasMemberId(member.getId(), joinedUserId);
	}

	@Override
	public void addMember(User member) {
		setJoinedUserId(MemberDocument.addMemberId(member.getId(), joinedUserId).list);
	}

	@Override
	public void removeMember(User member) {
		setJoinedUserId(MemberDocument.removeMemberId(member.getId(), joinedUserId).list);
	}

	@Override
	public List<ObjectId> getMemberIds(Class<? extends User> clazz) {
		return getJoinedUserId();
	}
	
	public List<ObjectId> getJoinedUserId() {
		return joinedUserId;
	}

	public void setJoinedUserId(List<ObjectId> joinedUserId) {
		this.joinedUserId = joinedUserId;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public Boolean getDatabaseDefault() {
		return databaseDefault;
	}

	public void setDatabaseDefault(Boolean databaseDefault) {
		this.databaseDefault = databaseDefault;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		return getIdentifier();
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		setIdentifier(identifier);
	}
}
