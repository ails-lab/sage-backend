package ac.software.semantic.service.container;

import java.util.HashMap;
import java.util.Map;

import ac.software.semantic.model.DataService;
import ac.software.semantic.model.DataServiceParameter;
import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.ParametricDocument;
import ac.software.semantic.model.base.EnclosingDocument;
import ac.software.semantic.model.index.IndexKeyMetadata;
import ac.software.semantic.model.state.ExecuteState;
import ac.software.semantic.payload.response.Response;
import ac.software.semantic.service.ExecutionOptions;
import ac.software.semantic.service.SideSpecificationDocument;

public interface DataServiceContainer<D extends SideSpecificationDocument & ParametricDocument, F extends Response, M extends ExecuteState, I extends EnclosingDocument> extends SideExecutableContainer<D,F,M,I> {

	public DataService getDataService();
	
	default void applyParameters(Map<String, Object> params) {
		
		Map<String, DataServiceParameter> map = new HashMap<>();
		DataService ds = getDataService();
		if (ds != null) {
			for (DataServiceParameter dsp : getDataService().getParameters()) {
				map.put(dsp.getName(), dsp);
			}
		}
		
		for (DataServiceParameterValue dsp : getObject().getParameters()) {
			params.put(dsp.getName(), dsp.getValue());
			
			map.remove(dsp.getName());
		}
		
		for (DataServiceParameter dsp : map.values()) {
			params.put(dsp.getName(), dsp.getDefaultValue() != null ? dsp.getDefaultValue() : "");
		}
	}
	
	public ExecutionOptions buildExecutionParameters();
	
	public String applyPreprocessToMappingDocument(ExecutionOptions eo) throws Exception ;
}
