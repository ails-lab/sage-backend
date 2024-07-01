package ac.software.semantic.payload.response;

import ac.software.semantic.payload.RefractoredAnnotationEditDetails;
import edu.ntua.isci.ac.d2rml.model.RDFTerm;

import java.util.List;

public class RefractoredAnnotationEditResponse {

    private RDFTerm propertyValue;

    private List<RefractoredAnnotationEditDetails> edits;

    public RDFTerm getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(RDFTerm propertyValue) {
        this.propertyValue = propertyValue;
    }

    public List<RefractoredAnnotationEditDetails> getEdits() {
        return edits;
    }

    public void setEdits(List<RefractoredAnnotationEditDetails> edits) {
        this.edits = edits;
    }
}
