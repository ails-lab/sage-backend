package ac.software.semantic.payload;

public class ByteData {

	private byte[] data;
	private String fileName;
	
	public ByteData(byte[] data, String fileName) {
		this.data = data;
		this.fileName = fileName;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public void setData(byte[] data) {
		this.data = data;
	}
	
	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
