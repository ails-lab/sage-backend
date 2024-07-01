package ac.software.semantic.controller.utils;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileRead {
	private String content;
	private int lines;
	private int nextLine;
	private int shard;
	
	FileRead(String content, int lines, int shard, int nextLine) {
		this.content = content;
		this.nextLine = nextLine;
		this.lines = lines;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public int getNextLine() {
		return nextLine;
	}

	public void setNextLine(int nextLine) {
		this.nextLine = nextLine;
	}

	public int getLines() {
		return lines;
	}

	public void setLines(int lines) {
		this.lines = lines;
	}
	
	public FileRead merge(FileRead fr) {
		return new FileRead(content + System.getProperty("line.separator") + fr.content, lines + fr.lines, fr.shard, fr.nextLine);
	}

	public int getShard() {
		return shard;
	}

	public void setShard(int shard) {
		this.shard = shard;
	}
}