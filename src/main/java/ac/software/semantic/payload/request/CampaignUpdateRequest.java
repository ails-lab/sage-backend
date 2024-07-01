package ac.software.semantic.payload.request;

import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;

public class CampaignUpdateRequest implements UpdateRequest {

	private String name; 
	
	private CampaignType type;
	private CampaignState state;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public CampaignState getState() {
		return state;
	}

	public void setState(CampaignState state) {
		this.state = state;
	}

	public CampaignType getType() {
		return type;
	}

	public void setType(CampaignType type) {
		this.type = type;
	}

	
}