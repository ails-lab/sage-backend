package ac.software.semantic.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway.Command;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.sftp.gateway.SftpOutboundGateway;
import org.springframework.integration.sftp.outbound.SftpMessageHandler;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;

import com.jcraft.jsch.ChannelSftp.LsEntry;

import ac.software.semantic.model.VirtuosoConfiguration;

@Configuration
public class SFTPAdaptor {

	private final static Logger logger = LoggerFactory.getLogger(SFTPAdaptor.class);

	@Autowired
	@Qualifier("virtuoso-configuration")
	private Map<String, VirtuosoConfiguration> virtuosoConfigurations;

	@Autowired
	private ApplicationContext ac;

	@Bean
	public Map<VirtuosoConfiguration, SessionFactory<LsEntry>> sftpSessionFactory() {
		Map<VirtuosoConfiguration, SessionFactory<LsEntry>> res = new HashMap<>();

		for (VirtuosoConfiguration vc : virtuosoConfigurations.values()) {
			DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);

			if (vc.getFileServer() != null) {
				factory.setHost(vc.getFileServer());
				factory.setUser(vc.getSftpUsername());
				factory.setPassword(vc.getSftpPassword());

				factory.setAllowUnknownKeys(true);

				res.put(vc, new CachingSessionFactory<LsEntry>(factory));
			}
		}

		return res;
	}

	@Bean(name = "upload-message-handler")
	@Scope("prototype")
	public MessageHandler uploadMessageHandler(VirtuosoConfiguration vc) {

		Map<VirtuosoConfiguration, SessionFactory<LsEntry>> vcFactories = sftpSessionFactory();
		SftpMessageHandler handler = new SftpMessageHandler(vcFactories.get(vc));

		handler.setRemoteDirectoryExpression(new LiteralExpression(vc.getUploadFolder()));
		handler.setFileNameGenerator(new FileNameGenerator() {

			@Override
			public String generateFileName(Message<?> message) {
				if (message.getPayload() instanceof File) {
					return ((File) message.getPayload()).getName();
				} else {
					throw new IllegalArgumentException("File expected as payload.");
				}
			}

		});
		return handler;
	}

	@Bean(name = "delete-message-handler")
	@Scope("prototype")
	public MessageHandler deleteMessageHandler(VirtuosoConfiguration vc) {

		Map<VirtuosoConfiguration, SessionFactory<LsEntry>> vcFactories = sftpSessionFactory();

		return new SftpOutboundGateway(vcFactories.get(vc), Command.RM.getCommand(), "'" + vc.getUploadFolder() + "/' + payload");
	}

	@ServiceActivator(inputChannel = "sftp.file.upload.request.channel")
	public void uploadHandler(Message<File> msg) {
		VirtuosoConfiguration vc = (VirtuosoConfiguration) msg.getHeaders().get("vc");

		SftpMessageHandler handler = (SftpMessageHandler) ac.getBean("upload-message-handler", vc);
		
		logger.info("Uploading " + msg.getPayload().getPath() + " to " + vc.getName());

		handler.handleMessage(msg);
	}

	@ServiceActivator(inputChannel = "sftp.file.delete.request.channel")
	public void deleteHandler(Message<String> msg) {
		VirtuosoConfiguration vc = (VirtuosoConfiguration) msg.getHeaders().get("vc");

		SftpOutboundGateway handler = (SftpOutboundGateway) ac.getBean("delete-message-handler", vc);
		
		logger.info("Deleting " + msg.getPayload() + " from " + vc.getName());

		handler.handleMessage(msg);
	}
	
	@MessagingGateway
	public interface SftpUploadGateway {

		@Gateway(requestChannel = "sftp.file.upload.request.channel")
		void upload(File file, @Header("vc") VirtuosoConfiguration vc);
	}

	@MessagingGateway 
	public interface SftpDeleteGateway {

		@Gateway(requestChannel = "sftp.file.delete.request.channel")
		CompletableFuture<Message<Boolean>> deleteFile(String file, @Header("vc") VirtuosoConfiguration vc);
	}

}
