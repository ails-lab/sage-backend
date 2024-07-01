package ac.software.semantic.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import ac.software.semantic.model.constants.type.MessageType;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationMessage {
	private String triplesMap;
	private String dataSource;
	private String key;
	
	private Integer index;
	private String message;
	
	private MessageType type;

	public NotificationMessage() {}

	public NotificationMessage(MessageType type, String message) {
		this(type, null, null, null, null, message);
	}

	public NotificationMessage(MessageType type, String triplesMap, String dataSource, String key, Integer index, String message) {
		this.type = type;
		this.triplesMap = triplesMap;
		this.dataSource = dataSource;
		this.key = key;
		this.index = index;
		this.message = message;
	}
	
	
	public Integer getIndex() {
		return index;
	}
	
	public void setIndex(Integer index) {
		this.index = index;
	}
	
	public String getMessage() {
		return message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}

	public String getTriplesMap() {
		return triplesMap;
	}

	public void setTriplesMap(String triplesMap) {
		this.triplesMap = triplesMap;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}
	
}