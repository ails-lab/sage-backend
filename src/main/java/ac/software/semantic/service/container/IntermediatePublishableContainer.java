package ac.software.semantic.service.container;

import java.util.Properties;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.Dataset;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.TripleStoreConfiguration;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.base.PublishableDocument;
import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.model.constants.state.DatasetState;
import ac.software.semantic.model.state.DatasetPublishState;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.model.state.PublishState;
import ac.software.semantic.payload.response.Response;

public interface IntermediatePublishableContainer<D extends SpecificationDocument, F extends Response, E extends ExecuteState, P extends PublishState<E>, I extends EnclosingDocument> extends EnclosedBaseContainer<D,F,I> {

	public D getObject();
	
	default public PublishableDocument<E, P> getPublishDocument() {
		return (PublishableDocument<E, P>)getObject();
	}
	
	default public ProcessStateContainer<P> getCurrentPublishState() {
		return getPublishDocument().getCurrentPublishState(getVirtuosoConfigurations().values());
	}
	
	public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations();

	default public TripleStoreConfiguration getDatasetTripleStoreVirtuosoConfiguration() {
		ProcessStateContainer<DatasetPublishState> psv = ((Dataset)getEnclosingObject()).getCurrentPublishState(getVirtuosoConfigurations().values());
		if (psv != null) {
			return psv.getTripleStoreConfiguration();
		} else {
			return null;
		}
	}
	
	default public boolean isPublished() {
		ProcessStateContainer<P> psc = getPublishDocument().getCurrentPublishState(getVirtuosoConfigurations().values());
		if (psc != null) {
			P ps = psc.getProcessState();
			if (ps.getPublishState() == DatasetState.PUBLISHED_PUBLIC || ps.getPublishState() == DatasetState.PUBLISHED_PRIVATE || ps.getPublishState() == DatasetState.PUBLISHED) {
				return true;
			}
		} 
			
		return false;
	}
	
	default public boolean isPublished(Properties props) {
		return isPublished();
	}
	
	default public boolean isFailed() {
		ProcessStateContainer<P> psc = getPublishDocument().getCurrentPublishState(getVirtuosoConfigurations().values());
		if (psc != null) {
			P ps = psc.getProcessState();
			if (ps.getPublishState() == DatasetState.UNPUBLISHING_FAILED || ps.getPublishState() == DatasetState.PUBLISHING_FAILED) {
				return true;
			}
		} 
			
		return false;
	}
	
	default public boolean isFailed(Properties props) {
		return isFailed();
	}
}
