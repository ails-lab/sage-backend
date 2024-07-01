package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Document(collection = "Users")
public class User implements IdentifiableDocument {
   
    @Id
    private ObjectId id;
    
    private List<ObjectId> databaseId;

    private String email;
    private String bCryptPassword;
    
    private String uuid;
    
    private String name;

    private Boolean multiLogin;
    private Date lastLogin;
    
    private String identifier;
    
    public User() { }
    
    public User(Database database) {  
    	this();

		this.uuid = UUID.randomUUID().toString();
		
		this.addDatabaseId(database.getId());
    }

    public Date getLastLogin() {
        return lastLogin;
    }

    public void setLastLogin(Date lastLogin) {
            this.lastLogin = lastLogin;
    }
    
    public ObjectId getId() {
        return id;
    }

    public String getEmail() {
        return email;
    }

    public String getBCryptPassword() {
        return bCryptPassword;
    }

    public String getUuid() {
        return uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
            this.name = name;
    }
   
    public List<ObjectId> getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(List<ObjectId> databaseId) {
        this.databaseId = databaseId;
    }

    public void addDatabaseId(ObjectId databaseId) {
    	if (this.databaseId == null) {
    		this.databaseId = new ArrayList<>();
    	}
        this.databaseId.add(databaseId);
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getbCryptPassword() {
        return bCryptPassword;
    }

    public void setbCryptPassword(String bCryptPassword) {
        this.bCryptPassword = bCryptPassword;
    }
    
    @Override
    public String toString() {
        return String.format("User{id='%s', email='%s', bCryptPassword=%s}\n",
                id, email, bCryptPassword);
    }

	@Override
	public ObjectId getUserId() {
		return id;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public Boolean getMultiLogin() {
		return multiLogin;
	}

	public void setMultiLogin(Boolean multiLogin) {
		this.multiLogin = multiLogin;
	}

	@Override
	public String getIdentifier(IdentifierType type) {
		if (type == IdentifierType.IDENTIFIER) {
			return getIdentifier();
		} else if (type == IdentifierType.EMAIL) {
			return getEmail();
		} 
		
		return null;
	}

	@Override
	public void setIdentifier(String identifier, IdentifierType type) {
		if (type == IdentifierType.IDENTIFIER) {
			setIdentifier(identifier);
		} else if (type == IdentifierType.EMAIL) {
			setEmail(identifier);
		} 
	}
}