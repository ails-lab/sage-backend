package ac.software.semantic.payload.notification;

import ac.software.semantic.model.constants.notification.NotificationChannel;

public class MessageObject {

	protected int order;
	
	protected NotificationChannel channel;

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	public NotificationChannel getChannel() {
		return channel;
	}

	public void setChannel(NotificationChannel channel) {
		this.channel = channel;
	}


}
