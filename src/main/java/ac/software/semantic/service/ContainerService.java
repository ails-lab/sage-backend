package ac.software.semantic.service;

import org.bson.types.ObjectId;

import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.FilterAnnotationValidationService.FilterAnnotationValidationContainer;

public interface ContainerService {

//	public ObjectContainer getContainer(UserPrincipal currentUser, ObjectId id);
	
	public ObjectContainer getContainer(UserPrincipal currentUser, ObjectIdentifier objId);
	
	public Class<? extends ObjectContainer> getContainerClass();
	
	public String synchronizedString(String id);

	
}
