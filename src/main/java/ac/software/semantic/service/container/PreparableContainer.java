package ac.software.semantic.service.container;

import ac.software.semantic.model.constants.state.PrepareState;

public interface PreparableContainer {

	public PrepareState prepare() throws Exception;
	
	public PrepareState isPrepared() throws Exception;
}
