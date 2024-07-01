package ac.software.semantic.controller.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileUtils {
	
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

//	private static int maxLines = 20000;
	public static int maxLines = 500;

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
	

    
    public static FileRead readFileLines(Path path, int shard, int offset) throws Exception {

		try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
			
			try (StringWriter sw = new StringWriter();
			     BufferedWriter bw = new BufferedWriter(sw)) {

				boolean eof = false;
				for (int i = 0; i < offset; i++) {
			        if (reader.readLine() == null) {
			        	eof = true;
			        	break;
			        }
				}

				String file = "";
				int count = 0;
				if (!eof) {
	    			while (count++ < maxLines) {
	    				String line = reader.readLine();
	    				if (line != null) {
	        				bw.append(line);
	        				bw.newLine();
	        			} else {
	        				eof = true;
	        				break;
	        			}
	    			}
	    			
	    			bw.flush();
	    			
	    			file = sw.toString();
	    			
	    			count--;
				}

				if (!eof) {
					return new FileRead(file, count, shard, offset + count);
				} else {
					return new FileRead(file, count, shard, -1);
				}
				
			}
		}
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
