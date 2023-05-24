package ac.software.semantic.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "Users")
public class User {
   
    @Id
    private ObjectId id;
    
    private List<ObjectId> databaseId;

    private String email;
    private String bCryptPassword;
    
    private String uuid;
    
    private String name;

    private List<ObjectId> validatorList;

    private Date lastLogin;
    
    public User() {  }
   
    public User(String email, String bCryptPassword, String uuid, String name) {
        this.email = email;
        this.bCryptPassword = bCryptPassword;
        this.uuid = uuid;
        
        this.databaseId = new ArrayList<>();
        this.name = name;
        this.validatorList = new ArrayList<>();
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


    public List<ObjectId> getValidatorList() {
        return validatorList;
    }

    public void setValidatorList(List<ObjectId> validatorList) {
        this.validatorList = validatorList;
    }

    public void addValidator(ObjectId id) {
        this.validatorList.add(id);
    }
    
    public List<ObjectId> getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(List<ObjectId> databaseId) {
        this.databaseId = databaseId;
    }

    public void addDatabaseId(ObjectId databaseId) {
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

}