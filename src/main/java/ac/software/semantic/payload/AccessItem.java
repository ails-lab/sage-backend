package ac.software.semantic.payload;

import ac.software.semantic.model.Access;

public class AccessItem {
    private String accessId;
    private String editorId;
    private String validatorId;
    private String datasetId;
    private String datasetUuid;

    public AccessItem(String accessId, String editorId, String validatorId, String datasetId, String datasetUuid) {
        this.accessId = accessId;
        this.editorId = editorId;
        this.validatorId = validatorId;
        this.datasetId = datasetId;
        this.datasetUuid = datasetUuid;
    }

    public AccessItem(Access access) {
        this.accessId = access.getId().toString();
        this.editorId = access.getCreatorId().toString();
        this.validatorId = access.getUserId().toString();
        this.datasetId = access.getCollectionId().toString();
        this.datasetUuid = access.getCollectionUuid();
    }

    public String getAccessId() {
        return this.accessId;
    }
    public String getEditorId() {
        return this.editorId;
    }
    public String getValidatorId() {
        return this.validatorId;
    }
    public String getDatasetId() {
        return this.datasetId;
    }

    public String getDatasetUuid() {
        return datasetUuid;
    }

    public void setDatasetUuid(String datasetUuid) {
        this.datasetUuid = datasetUuid;
    }
}