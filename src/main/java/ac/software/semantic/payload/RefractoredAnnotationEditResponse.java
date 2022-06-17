package ac.software.semantic.payload;

import ac.software.semantic.model.AnnotationEditValue;

import java.util.List;

public class RefractoredAnnotationEditResponse {

    private AnnotationEditValue propertyValue;

    private List<RefractoredAnnotationEditDetails> edits;

    public AnnotationEditValue getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(AnnotationEditValue propertyValue) {
        this.propertyValue = propertyValue;
    }

    public List<RefractoredAnnotationEditDetails> getEdits() {
        return edits;
    }

    public void setEdits(List<RefractoredAnnotationEditDetails> edits) {
        this.edits = edits;
    }
}
