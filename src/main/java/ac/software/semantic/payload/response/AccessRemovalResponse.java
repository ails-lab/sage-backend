package ac.software.semantic.payload.response;

public class AccessRemovalResponse {
    private Long entriesRemoved;

    public AccessRemovalResponse(Long entriesRemoved) {
        this.entriesRemoved = entriesRemoved;
    }

    public Long getEntriesRemoved() {
        return entriesRemoved;
    }

    public void setEntriesRemoved(Long entriesRemoved) {
        this.entriesRemoved = entriesRemoved;
    }
}

