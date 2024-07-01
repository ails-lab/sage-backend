package ac.software.semantic.payload.response.modifier;

import java.util.List;

public interface ResponseModifier {
	
	public static <T extends ResponseModifier> T createModifier(Class<T> clazz, List<String> options)  {
		T rm = null;
		
		try {
			if (options.contains("base")) {
				rm = (T)clazz.getMethod("baseModifier").invoke(null);
			} else if (options.contains("full")) {
				rm = (T)clazz.getMethod("fullModifier").invoke(null);
			} else {
				rm = (T)clazz.getConstructor().newInstance();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		rm.customizeModifier(options);
		
		return rm;
	}
	
	public default void customizeModifier(List<String> options) {
	}

}
