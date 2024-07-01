package ac.software.semantic.model;

import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.TokenState;
import ac.software.semantic.model.constants.type.TokenType;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

@Document(collection = "ActionTokens")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActionToken {
    @Id
    private ObjectId id;
    private String token;
    
    private ObjectId userId;
    
    @JsonIgnore
    private ObjectId databaseId;
    
    private Date createdAt;
    private Date expiryDate;
    
    private TokenType type;
    
    private TokenState state;
    private Date usedAt;
    
    private TokenDetails scope; 

	private ActionToken() {
		token = UUID.randomUUID().toString();
	}

	public ActionToken(Database database) {
		this();
		
		this.databaseId = database.getId();
	}
	
	public void setExpiration(int durationInMinutes) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(createdAt.getTime());
        cal.add(Calendar.MINUTE, durationInMinutes);
            
        setExpiryDate(new Date(cal.getTime().getTime()));
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Date getExpiryDate() {
        return expiryDate;
    }

    public void setExpiryDate(Date expiryDate) {
        this.expiryDate = expiryDate;
    }

	public TokenType getType() {
		return type;
	}

	public void setType(TokenType type) {
		this.type = type;
	}
	
    public boolean hasExpired() {
		if (Calendar.getInstance().getTime().before(getExpiryDate())) {
			return false;
		} else {
			return true;
		}
    }


	public ObjectId getUserId() {
		return userId;
	}

	public void setUserId(ObjectId userId) {
		this.userId = userId;
	}

	public Date getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	public TokenDetails getScope() {
		return scope;
	}

	public void setScope(TokenDetails scope) {
		this.scope = scope;
	}

	public ObjectId getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(ObjectId databaseId) {
		this.databaseId = databaseId;
	}

	public TokenState getState() {
		return state;
	}

	public void setState(TokenState state) {
		this.state = state;
	}

	public Date getUsedAt() {
		return usedAt;
	}

	public void setUsedAt(Date usedAt) {
		this.usedAt = usedAt;
	}
	
}
