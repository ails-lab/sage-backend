package ac.software.semantic.model;

import java.util.Date;

public interface DatedDocument {

	public Date getCreatedAt();

	public void setCreatedAt(Date createdAt);
	
	public Date getUpdatedAt();

	public void setUpdatedAt(Date updatedAt);

}
