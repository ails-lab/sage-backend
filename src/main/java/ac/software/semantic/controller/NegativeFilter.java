package ac.software.semantic.controller;

public class NegativeFilter {

	public NegativeFilter() {
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof Integer)) {
			return false;
		}
		return ((int) obj) == -1;
	}
}
