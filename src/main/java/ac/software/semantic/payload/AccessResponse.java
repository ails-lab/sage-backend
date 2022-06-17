package ac.software.semantic.payload;

import java.util.List;

public class AccessResponse {

    private List<AccessItem> access;

    public AccessResponse(List<AccessItem> access) {
        this.access = access;
    }

    public List<AccessItem> getAccess() {
        return this.access;
    }
}