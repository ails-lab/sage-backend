package ac.software.semantic.payload.response;

public interface ExecutePublishResponse extends ExecuteResponse, PublishResponse {
	public Boolean isPublishedFromCurrentFileSystem();

	public void setPublishedFromCurrentFileSystem(Boolean publishedFromCurrentFileSystem);

	public Boolean isNewExecution();

	public void setNewExecution(Boolean newExecution);
}
