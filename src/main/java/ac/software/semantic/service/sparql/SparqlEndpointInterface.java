package ac.software.semantic.service.sparql;

public interface SparqlEndpointInterface extends AutoCloseable {

	public void executeSelect(String location, String sparql);
	
	public boolean hasNext();
	
	public void next();
}
