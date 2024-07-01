package ac.software.semantic.payload.request;

import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileUpdateRequest implements MultipartFileUpdateRequest, InGroupRequest {

	private String name; 
	private String url;
	
	private String description;
	
	private Boolean active;
	private int group;
	
	@JsonIgnore
	private MultipartFile file;

	public FileUpdateRequest() { }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public MultipartFile getFile() {
		return file;
	}

	@Override
	public void setFile(MultipartFile file) {
		this.file = file;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Boolean isActive() {
		return active;
	}

	public void setActive(Boolean active) {
		this.active = active;
	}
	
	@Override
	public int getGroup() {
		return group;
	}

	@Override
	public void setGroup(int group) {
		this.group = group;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}