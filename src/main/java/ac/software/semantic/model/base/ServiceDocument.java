package ac.software.semantic.model.base;

import ac.software.semantic.model.DataService.DataServiceType;

public interface ServiceDocument extends SpecificationDocument {
	
	public String getVariant();
	
	public String getIdentity();
	
	public DataServiceType getType();

}
