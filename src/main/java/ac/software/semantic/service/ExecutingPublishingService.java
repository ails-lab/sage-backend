package ac.software.semantic.service;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.payload.response.Response;

public interface ExecutingPublishingService<D extends SpecificationDocument, F extends Response> extends ExecutingService<D,F>, PublishingService<D,F> {

}
