package ac.software.semantic.service;

import java.util.Properties;

import ac.software.semantic.model.base.SpecificationDocument;
import ac.software.semantic.service.container.ObjectContainer;

@FunctionalInterface
public interface Condition<D extends SpecificationDocument> {

	public boolean evaluate(Properties props, ObjectContainer<D,?> container);
}
