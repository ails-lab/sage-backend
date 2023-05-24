package ac.software.semantic.vocs;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import ac.software.semantic.model.Database;
import ac.software.semantic.model.IRICompatibility;
import ac.software.semantic.model.constants.PathElementType;

@Service
public class LegacyVocabulary {

	@Autowired
    @Qualifier("database")
	Database database;

    public Property fixLegacy(Property current) {
    	if (database.getCompatibility() != null) {
	   		for (IRICompatibility comp : database.getCompatibility().getIris()) {
	   			if (comp.getType() == PathElementType.PROPERTY && comp.getCurrent().equals(current.toString())) {
	   				return current.getModel().createProperty(comp.getLegacy());
	   			}
	   		}
    	}
    	
   		return current;
    }

    public Resource fixLegacy(Resource current) {
    	if (database.getCompatibility() != null) {
	   		for (IRICompatibility comp : database.getCompatibility().getIris()) {
	   			if (comp.getType() == PathElementType.CLASS && comp.getCurrent().equals(current.toString())) {
	   				return current.getModel().createProperty(comp.getLegacy());
	   			}
	   		}
    	}
    	
   		return current;
    }
    
    
}

