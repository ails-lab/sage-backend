package ac.software.semantic.payload.response;

import ac.software.semantic.model.PagedAnnotationValidationPage;

public class UpdateLockedPageResponse {
    boolean error;
    PagedAnnotationValidationPage page;

    public UpdateLockedPageResponse(boolean error, PagedAnnotationValidationPage page) {
        this.error = error;
        this.page = page;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public PagedAnnotationValidationPage getPage() {
        return page;
    }

    public void setPage(PagedAnnotationValidationPage page) {
        this.page = page;
    }
}
