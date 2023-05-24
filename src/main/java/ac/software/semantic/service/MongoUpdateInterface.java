package ac.software.semantic.service;

@FunctionalInterface
public interface MongoUpdateInterface {

	public void update(ObjectContainer oc) throws Exception;

}
