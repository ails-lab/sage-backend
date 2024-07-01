package ac.software.semantic.service;

import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.data.domain.Pageable;

import ac.software.semantic.model.ListPage;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.security.UserPrincipal;
import ac.software.semantic.service.container.ObjectContainer;

public interface EnclosingService<D extends SpecificationDocument, F extends Response, I extends EnclosingDocument> extends ContainerService<D,F> {

	@Override
	default public ObjectContainer<D,F> getContainer(UserPrincipal currentUser, D object) {
		return getContainer(currentUser, object, null);
	}

	public ObjectContainer<D,F> getContainer(UserPrincipal currentUser, D object, I dataset);
	
	public ListPage<D> getAllByUser(List<I> dataset, ObjectId userId, Pageable page);
}
