package ac.software.semantic.payload;

import ac.software.semantic.model.AnnotationEditType;
import org.springframework.data.annotation.Id;

public class RefractoredAnnotationEditDetails {
    @Id
    private String id;
    private String annotationValue;
    private AnnotationEditType editType;
    private int start;
    private int end;
    private int count;

    public RefractoredAnnotationEditDetails() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAnnotationValue() {
        return annotationValue;
    }

    public void setAnnotationValue(String annotationValue) {
        this.annotationValue = annotationValue;
    }

    public AnnotationEditType getEditType() {
        return editType;
    }

    public void setEditType(AnnotationEditType editType) {
        this.editType = editType;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
