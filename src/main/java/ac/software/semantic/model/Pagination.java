package ac.software.semantic.model;

import org.springframework.data.domain.Page;

public class Pagination {

	private int currentPage;
	private int currentElements;
	
	private int totalPages;
	private long totalElements;
	
	public Pagination() {
		
	}
	
	public int getCurrentPage() {
		return currentPage;
	}
	
	public void setCurrentPage(int page) {
		this.currentPage = page;
	}

	public int getCurrentElements() {
		return currentElements;
	}

	public void setCurrentElements(int pageSize) {
		this.currentElements = pageSize;
	}

	public int getTotalPages() {
		return totalPages;
	}

	public void setTotalPages(int numberOfPages) {
		this.totalPages = numberOfPages;
	}

	public long getTotalElements() {
		return totalElements;
	}

	public void setTotalElements(long numberOfElements) {
		this.totalElements = numberOfElements;
	}
	
	public static Pagination fromPage(Page<?> page) {
		Pagination pg = new Pagination();
		pg.setTotalElements(page.getTotalElements());
		pg.setTotalPages(page.getTotalPages());
		pg.setCurrentPage(page.getNumber() + 1);
		pg.setCurrentElements(page.getNumberOfElements());
		
		return pg;
	}

}
