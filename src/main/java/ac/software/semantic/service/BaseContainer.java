package ac.software.semantic.service;

import org.bson.types.ObjectId;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.security.UserPrincipal;

public interface BaseContainer {

	public UserPrincipal getCurrentUser();
	
	public Dataset getDataset();
	
	public void save(MongoUpdateInterface mui) throws Exception;
	
	public boolean delete() throws Exception;
	
	public abstract ObjectId getPrimaryId();
	
	public abstract ObjectId getSecondaryId();
}
