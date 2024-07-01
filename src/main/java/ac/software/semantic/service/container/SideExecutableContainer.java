package ac.software.semantic.service.container;

import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.SideSpecificationDocument;

public interface SideExecutableContainer<D extends SideSpecificationDocument, F extends Response, M extends ExecuteState, I extends EnclosingDocument> extends ExecutableContainer<D, F, M,I> {
	
}
