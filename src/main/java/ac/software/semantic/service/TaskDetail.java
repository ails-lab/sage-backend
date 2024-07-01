package ac.software.semantic.service;

import ac.software.semantic.model.constants.type.DocumentType;
import ac.software.semantic.model.constants.type.OperationType;
import ac.software.semantic.model.constants.type.TargetType;

public class TaskDetail {
	private DocumentType documentType;
	private OperationType operationType;
	private TargetType targetType;
	
	public TaskDetail (DocumentType documentType, OperationType operationType) {
		this(documentType, operationType, null);
	}

	public TaskDetail (DocumentType documentType, OperationType operationType, TargetType targetType) {
		this.documentType = documentType;
		this.operationType = operationType;
		this.targetType = targetType;
	}

	public DocumentType getDocumentType() {
		return documentType;
	}

	public void setDocumentType(DocumentType documentType) {
		this.documentType = documentType;
	}

	public OperationType getOperationType() {
		return operationType;
	}

	public void setOperationType(OperationType operationType) {
		this.operationType = operationType;
	}

	public TargetType getTargetType() {
		return targetType;
	}

	public void setTargetType(TargetType targetType) {
		this.targetType = targetType;
	}
	
}