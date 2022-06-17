package ac.software.semantic.payload;

import java.util.List;

public class ValidatorResponse {
    private List<ValidatorItem> validators;

    public ValidatorResponse(List<ValidatorItem> validators) {
        this.validators = validators;
    }

    public List<ValidatorItem> getValidators() {
        return validators;
    }

    public void setValidators( List<ValidatorItem> validators) {
        this.validators = validators;
    }

}