package ac.software.semantic.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;



public class PathElement {
	private String type;
	private String uri;
	
	public PathElement(String type, String uri) {
		this.type = type;
		this.uri = uri;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public static String[] onPropertyList(List<PathElement> path) {
		// TODO: check if in right order.
		List<String> spath = new ArrayList<>();
		for (int i = 0; i < path.size(); i++) {
			if (path.get(i).type.equals("class")) {
				continue;
			}

			spath.add(path.get(i).uri);
		}

		return spath.toArray(new String[] {});
	}

	public static String onPropertyListAsString(List<PathElement> path) {
		// TODO: check if in right order.
		String spath = "";
		for (int i = 0; i < path.size(); i++) {
			if (path.get(i).type.equals("class")) {
				continue;
			}

			if (spath.length() > 0) {
				spath += "/";
			}
			spath += "<" + path.get(i).uri + ">";
		}

		return spath;
	}
}