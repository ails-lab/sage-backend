package ac.software.semantic.service.lookup;

import org.bson.types.ObjectId;

import ac.software.semantic.model.constants.state.CampaignState;
import ac.software.semantic.model.constants.type.CampaignType;

public class CampaignLookupProperties implements LookupProperties {
	
	private CampaignType campaignType;
	private CampaignState campaignState;
	
	private ObjectId validatorId;
	
	public CampaignType getCampaignType() {
		return campaignType;
	}

	public void setCampaignType(CampaignType campaignType) {
		this.campaignType = campaignType;
	}

	public CampaignState getCampaignState() {
		return campaignState;
	}

	public void setCampaignState(CampaignState campaignState) {
		this.campaignState = campaignState;
	}

	public ObjectId getValidatorId() {
		return validatorId;
	}

	public void setValidatorId(ObjectId validatorId) {
		this.validatorId = validatorId;
	}

}
