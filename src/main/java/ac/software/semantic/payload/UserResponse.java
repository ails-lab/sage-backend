package ac.software.semantic.payload;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.UserRoleType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserResponse {

	private String id;
	
    private String email;
    private String name;
    
    private List<UserRoleType> roles;
    
    private Long datasetCount;
    private Long annotationEditAcceptCount;
    private Long annotationEditRejectCount;
    private Long annotationEditAddCount;
    
    private Boolean inOtherDatabases;
    
    
    public UserResponse() {
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

	public List<UserRoleType> getRoles() {
		return roles;
	}

	public void setRoles(List<UserRoleType> roles) {
		this.roles = roles;
	}

	public Long getDatasetCount() {
		return datasetCount;
	}

	public void setDatasetCount(Long datasetCount) {
		this.datasetCount = datasetCount;
	}

	public Long getAnnotationEditAcceptCount() {
		return annotationEditAcceptCount;
	}

	public void setAnnotationEditAcceptCount(Long annotationEditAcceptCount) {
		this.annotationEditAcceptCount = annotationEditAcceptCount;
	}

	public Long getAnnotationEditRejectCount() {
		return annotationEditRejectCount;
	}

	public void setAnnotationEditRejectCount(Long annotationEditRejectCount) {
		this.annotationEditRejectCount = annotationEditRejectCount;
	}

	public Long getAnnotationEditAddCount() {
		return annotationEditAddCount;
	}

	public void setAnnotationEditAddCount(Long annotationEditAddCount) {
		this.annotationEditAddCount = annotationEditAddCount;
	}

	public Boolean getInOtherDatabases() {
		return inOtherDatabases;
	}

	public void setInOtherDatabases(Boolean inOtherDatabases) {
		this.inOtherDatabases = inOtherDatabases;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
