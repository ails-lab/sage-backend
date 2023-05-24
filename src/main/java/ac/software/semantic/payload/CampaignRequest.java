package ac.software.semantic.payload;

import ac.software.semantic.model.constants.CampaignState;

public class CampaignRequest {

	private String name; 
	
	private CampaignState state;

	public CampaignRequest() { }

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

	
}