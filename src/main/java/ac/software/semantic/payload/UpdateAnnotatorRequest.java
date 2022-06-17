package ac.software.semantic.payload;

import ac.software.semantic.model.DataServiceParameterValue;
import ac.software.semantic.model.PreprocessInstruction;

import java.util.List;

public class UpdateAnnotatorRequest {
    private String asProperty;
    private String annotator;
    private String thesaurus;

    private String variant;

    private List<DataServiceParameterValue> parameters;
    private List<PreprocessInstruction> preprocess;

    public String getAsProperty() {
        return asProperty;
    }

    public void setAsProperty(String asProperty) {
        this.asProperty = asProperty;
    }

    public String getAnnotator() {
        return annotator;
    }

    public void setAnnotator(String annotator) {
        this.annotator = annotator;
    }

    public String getThesaurus() {
        return thesaurus;
    }

    public void setThesaurus(String thesaurus) {
        this.thesaurus = thesaurus;
    }

    public String getVariant() {
        return variant;
    }

    public void setVariant(String variant) {
        this.variant = variant;
    }

    public List<DataServiceParameterValue> getParameters() {
        return parameters;
    }

    public void setParameters(List<DataServiceParameterValue> parameters) {
        this.parameters = parameters;
    }

    public List<PreprocessInstruction> getPreprocess() {
        return preprocess;
    }

    public void setPreprocess(List<PreprocessInstruction> preprocess) {
        this.preprocess = preprocess;
    }
}
