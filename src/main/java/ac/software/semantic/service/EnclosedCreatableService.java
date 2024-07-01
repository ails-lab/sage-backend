package ac.software.semantic.service;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.request.UpdateRequest;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.security.UserPrincipal;

public interface EnclosedCreatableService<D extends SpecificationDocument, F extends Response, U extends UpdateRequest, I extends EnclosingDocument> extends EnclosingService<D,F,I> 
{

	public D create(UserPrincipal currentUser, I inside, U request) throws Exception;
}
