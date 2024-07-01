package ac.software.semantic.service;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import com.fasterxml.jackson.annotation.JsonInclude;

import edu.ntua.isci.ac.d2rml.model.RDFTerm;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SingleRDFTerm extends RDFTerm implements RDFTermHandler {

	private String name;
	
	public SingleRDFTerm(Literal literal) {
		super(literal);
	} 

	public SingleRDFTerm(Resource iri) {
		super(iri);
	} 
	
	public static SingleRDFTerm createRDFTerm(RDFNode node) {
		if (node instanceof Resource) {
			return new SingleRDFTerm(node.asResource());
		} else if (node instanceof Literal) {
			return new SingleRDFTerm(node.asLiteral());
		}
		
		return null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String toString() {
		return (name != null ? name + ":" : "") + super.toString(); 
	}

}
