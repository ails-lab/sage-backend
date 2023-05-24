package ac.software.semantic;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) throws Exception {
		

		Pattern p = Pattern.compile("^([^\\d]+?)\\s+([/0-9\\-]+?[a-z]?)(?:\\s+([A-Z]+?))?(?:\\s+([0-9]+))?$");
		
		Matcher m = p.matcher("Ilmarisenkatu 11");
		while (m.find()) {
			for (int i = 1; i <= m.groupCount(); i++) {
				System.out.println(i + " " + m.group(i));
			}
		}
		
		m = p.matcher("Skarvgränd 2");
		while (m.find()) {
			for (int i = 1; i <= m.groupCount(); i++) {
				System.out.println(i + " " + m.group(i));
			}
		}

//		Pattern p = Pattern.compile("^(?:([^\\\\d]+?), )?(?:([0-9]{4}), )?(.*?)$");
//
//		Matcher m = p.matcher("1097, Λευκωσία");
//		while (m.find()) {
//			for (int i = 1; i <= m.groupCount(); i++) {
//				System.out.println(i + " " + m.group(i));
//			}
//		}
//		
//		
//		m = p.matcher("Λάρνακα, 1097, Λευκωσία");
//		while (m.find()) {
//			for (int i = 1; i <= m.groupCount(); i++) {
//				System.out.println(i + " " + m.group(i));
//			}
//		}
//		
//		m = p.matcher("Λάρνακα, Λευκωσία");
//		while (m.find()) {
//			for (int i = 1; i <= m.groupCount(); i++) {
//				System.out.println(i + " " + m.group(i));
//			}
//		}
	}
}
