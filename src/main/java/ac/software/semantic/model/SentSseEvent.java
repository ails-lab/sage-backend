package ac.software.semantic.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "SENT_SSE_EVENT")
public class SentSseEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    
    private String channel;
    
    private long timestamp;
    
    @Column(columnDefinition="BINARY(1000)")
    private byte[] content;

    public long getId() {
        return id;
    }

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}
	
	public String toString() {
		return "SENT SSE EVENT : " + id + " " + timestamp + " " + channel + " " + new String(content);
	}

}