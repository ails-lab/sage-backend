package ac.software.semantic.service.container;

import org.bson.types.ObjectId;

import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.ContainerService;
import ac.software.semantic.service.UserService.UserContainer;

public interface BaseContainer<D extends SpecificationDocument, F extends Response> {

	public D getObject();
	
	public UserPrincipal getCurrentUser();
	
	public UserPrincipal getObjectCreator();
	
	public D update(MongoUpdateInterface<D,F> mui) throws Exception;
	
	// return true if deletion removes from list, false if removes internal elements (mapping instance)
	public boolean delete() throws Exception;
	
	public abstract ObjectId getPrimaryId();
	
	public abstract ObjectId getSecondaryId();
	
	public abstract F asResponse();

	public void setObjectOwner();
	
	default boolean isCurrentUserOwner() {
		UserPrincipal u = getCurrentUser();
		
		if (u == null) {
			return false;
		} else {
			return getObject().getUserId().toString().equals(u.getId());
		}
	}
	
	default UserPrincipal getObjectOwner() {
		if (getObjectCreator() == null) {
			setObjectOwner();
		} 
		
		return getObjectCreator();
	}
	
	public ContainerService<D,F> getService();

}
