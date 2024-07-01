package ac.software.semantic.payload.request;

import org.springframework.web.multipart.MultipartFile;

public interface MultipartFileUpdateRequest extends UpdateRequest {

	public MultipartFile getFile();

	public void setFile(MultipartFile file);
}
