package ac.software.semantic.controller.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import ac.software.semantic.service.ServiceUtils;

public class FileUtils {
	
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

	private static int maxLines = 20000;

    public static String readFileBeginning(Path path) {
    	String file = "";
    	BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(path.toFile()));
			
			try (StringWriter sw = new StringWriter();
			     BufferedWriter bw = new BufferedWriter(sw)) {

				String line = reader.readLine();
    			if (line != null) {
    				bw.append(line);
    				bw.newLine();
    			}
    			int count = 0;
    			while (line != null && count++ < maxLines) {
    				line = reader.readLine();
    				if (line != null) {
        				bw.append(line);
        				bw.newLine();
        			}	
    			}
    			
    			bw.flush();
    			file = sw.toString();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return file;

    }
    
	public static boolean deleteFile(File f) {
		boolean ok = false;
		if (f != null) {
			ok = f.delete();
			if (ok) {
				logger.info("Deleted file " + f.getAbsolutePath());
			} else {
				logger.warn("Failed to delete file + " + f.getAbsolutePath());
			}		

		}
		
		return ok;
	}
	
}
