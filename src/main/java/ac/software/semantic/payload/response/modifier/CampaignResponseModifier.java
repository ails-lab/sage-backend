package ac.software.semantic.payload.response.modifier;

import ac.software.semantic.payload.response.ResponseFieldType;

public class CampaignResponseModifier implements ResponseModifier {

	private ResponseFieldType validators;
	private ResponseFieldType datasets;
	
	public static CampaignResponseModifier fullModifier() {
		CampaignResponseModifier rm = new CampaignResponseModifier();
		rm.setValidators(ResponseFieldType.EXPAND);
		rm.setDatasets(ResponseFieldType.EXPAND);
		
		return rm;
	}
	
	public CampaignResponseModifier() {
		validators = ResponseFieldType.IGNORE;
		datasets = ResponseFieldType.IGNORE;
	}
	
	public ResponseFieldType getValidators() {
		return validators;
	}

	public void setValidators(ResponseFieldType validators) {
		this.validators = validators;
	}

	public ResponseFieldType getDatasets() {
		return datasets;
	}

	public void setDatasets(ResponseFieldType datasets) {
		this.datasets = datasets;
	}
}
