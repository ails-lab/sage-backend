package ac.software.semantic.model.constants.state;

public enum CampaignState {
	
	ACTIVE,
	INACTIVE
	;
	
	public static CampaignState get(String type) {
		if (type.equals("ACTIVE")) {
			return ACTIVE;
		} else if (type.equals("INACTIVE")) {
			return INACTIVE;
		} 
		
		return null;
	}
	
}
