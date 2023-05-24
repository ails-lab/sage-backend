package ac.software.semantic.model.constants;

public enum CampaignType {
    ANNOTATION_VALIDATION;

    public static CampaignType get(String type) {
    	if (type.equals("ANNOTATION_VALIDATION")) {
			return ANNOTATION_VALIDATION;
		}
		
		return null;
	}
}