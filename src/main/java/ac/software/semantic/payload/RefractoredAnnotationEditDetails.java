package ac.software.semantic.payload;

import java.util.List;

import org.springframework.data.annotation.Id;

import ac.software.semantic.model.constants.type.AnnotationEditType;

public class RefractoredAnnotationEditDetails {
    @Id
    private String id;
    private String annotationValue;
    private AnnotationEditType editType;
    private Integer start;
    private Integer end;
    private int count;
    
    private String target;
    
    private List<ValueAnnotationReference> references;

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

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getEnd() {
        return end;
    }

    public void setEnd(Integer end) {
        this.end = end;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public List<ValueAnnotationReference> getReferences() {
		return references;
	}

	public void setReferences(List<ValueAnnotationReference> references) {
		this.references = references;
	}
}
