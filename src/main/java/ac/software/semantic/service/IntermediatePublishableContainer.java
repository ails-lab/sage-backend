package ac.software.semantic.service;

import ac.software.semantic.config.ConfigurationContainer;
import ac.software.semantic.model.ProcessStateContainer;
import ac.software.semantic.model.PublishDocument;
import ac.software.semantic.model.TripleStoreConfiguration;

public interface IntermediatePublishableContainer extends BaseContainer {

	public PublishDocument<?> getPublishDocument();
	
	default public ProcessStateContainer getCurrentPublishState() {
		return getPublishDocument().getCurrentPublishState(getVirtuosoConfigurations().values());
	}
	public ConfigurationContainer<TripleStoreConfiguration> getVirtuosoConfigurations();

	default TripleStoreConfiguration getDatasetTripleStoreVirtuosoConfiguration() {
		ProcessStateContainer psv = getDataset().getCurrentPublishState(getVirtuosoConfigurations().values());
		if (psv != null) {
			return psv.getTripleStoreConfiguration();
		} else {
			return null;
		}
	}
	
}
