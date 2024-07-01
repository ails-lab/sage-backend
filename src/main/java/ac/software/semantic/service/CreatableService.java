package ac.software.semantic.service;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.security.UserPrincipal;

public interface CreatableService<D extends SpecificationDocument, F extends Response, U extends UpdateRequest> extends ContainerService<D,F> {

	public D create(UserPrincipal currentUser, U request) throws Exception;

}
