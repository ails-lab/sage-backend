package ac.software.semantic.service;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.DatedDocument;
import ac.software.semantic.model.IdentifiableDocument;
import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.type.IdentifierType;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.repository.DocumentRepository;
import ac.software.semantic.repository.IdentifiableDocumentRepository;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.ObjectContainer;
import ac.software.semantic.service.container.ObjectIdentifier;
import ac.software.semantic.service.exception.StateConflictException;

public interface ContainerService<D extends SpecificationDocument, F extends Response> {

	public ObjectContainer<D,F> getContainer(UserPrincipal currentUser, D object);
	
	public ObjectContainer<D,F> getContainer(UserPrincipal currentUser, ObjectIdentifier objId);
	
	public Class<? extends ObjectContainer<D,F>> getContainerClass();
	
	public String synchronizedString(String id);
	
	public ListPage<D> getAllByUser(ObjectId userId, Pageable page);

	// CAUTION: assumes any first implemented interface argument is the base class 
	default Class<D> getSpecificationDocumentClass() {
		try {
	        Type type = getClass().getGenericInterfaces()[0];
	        ParameterizedType paramType = (ParameterizedType)type;
	        Class<D> e = (Class<D>)paramType.getActualTypeArguments()[0];

			return e;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public abstract DocumentRepository<D> getRepository();
	
	public default D create(D object) throws Exception {
		DocumentRepository<D> repository = getRepository();

		if (object instanceof DatedDocument) {
			((DatedDocument) object).setCreatedAt(new Date());
		}

		D res;
		
		if (object instanceof IdentifiableDocument) {
			synchronized (object.getClass().toString().intern()) { // is this ok ?
				
				IdentifiableDocumentService<?,?> is = (IdentifiableDocumentService<?,?>)this;
				
				for (IdentifierType type : is.identifierTypes()) {
					
					String identifier = ((IdentifiableDocument)object).getIdentifier(type);
					
					if (identifier != null) {
						if (((IdentifiableDocumentRepository<D>)repository).existsSameIdentifier(object, type)) { 
							throw new StateConflictException("The identifier already exists.");
						}
					}
				}
				
				res = getRepository().save(object);
			}
		} else {
			res = getRepository().save(object);
		}
		
		return res;
	}
	
}
