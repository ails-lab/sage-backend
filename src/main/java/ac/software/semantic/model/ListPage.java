package ac.software.semantic.model;

import java.util.List;

import org.springframework.data.domain.Page;

import ac.software.semantic.model.base.SpecificationDocument;

public class ListPage<D extends SpecificationDocument> {
	
	private List<D> list;
	
	private Pagination pagination;
	
	public ListPage() {
	}
	
	public static <T extends SpecificationDocument> ListPage<T> create(List<T> list) {
		ListPage<T> lp = new ListPage<T>();
		lp.setList(list);
		return lp;
	}

	public static <T extends SpecificationDocument> ListPage<T> create(Page<T> page) {
		ListPage<T> lp = new ListPage<T>();
		lp.setList(page.getContent());
		lp.setPagination(Pagination.fromPage(page));
		return lp;
	}
	
	public List<D> getList() {
		return list;
	}
	
	public void setList(List<D> list) {
		this.list = list;
	}

	public Pagination getPagination() {
		return pagination;
	}

	public void setPagination(Pagination pagination) {
		this.pagination = pagination;
	}
	
}
