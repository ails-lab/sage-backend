package ac.software.semantic.payload;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

public class PublicEditorsResponse {
    private List<EditorItem> editors;

    public PublicEditorsResponse(List<EditorItem> editors) {
        this.editors = editors;
    }

    public List<EditorItem> getEditors() {
        return editors;
    }

    public void setEditors( List<EditorItem> editors) {
        this.editors = editors;
    }
}