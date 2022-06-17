package ac.software.semantic.controller;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class AsyncUtils {

    public static <T> CompletableFuture<T> supplyAsync(Callable<T> c) {
        CompletableFuture<T> f = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try { 
            	f.complete(c.call()); 
            } catch(Throwable t) { 
            	f.completeExceptionally(t); 
            }
        });
        return f;
    }

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
}

