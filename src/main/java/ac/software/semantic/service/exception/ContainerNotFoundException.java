package ac.software.semantic.service.exception;

import ac.software.semantic.service.container.ObjectContainer;

public class ContainerNotFoundException extends Exception {

	private Class<? extends ObjectContainer<?,?>> containerClass;
			
	public ContainerNotFoundException(Class<? extends ObjectContainer<?,?>> containerClass) {
		this.containerClass = containerClass;
	}

	public Class<? extends ObjectContainer<?,?>> getContainerClass() {
		return containerClass;
	}

	public void setContainerClass(Class<? extends ObjectContainer<?,?>> containerClass) {
		this.containerClass = containerClass;
	}
}
