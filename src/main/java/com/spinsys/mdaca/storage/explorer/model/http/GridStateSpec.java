package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.ArrayList;
import java.util.List;

public class GridStateSpec {

	private int startRow;
	private int endRow;
	private String sortField;
	private String sortDir;
	private List<GridFilter> filters;
	
	public int getStartRow() {
		return startRow;
	}
	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}
	
	public int getEndRow() {
		return endRow;
	}
	public void setEndRow(int endRow) {
		this.endRow = endRow;
	}
	public String getSortField() {
		return sortField;
	}
	public void setSortField(String sortField) {
		this.sortField = sortField;
	}
	public String getSortDir() {
		return sortDir;
	}
	public void setSortDir(String sortDir) {
		this.sortDir = sortDir;
	}
	public List<GridFilter> getFilters() {
		return filters;
	}
	public void setFilters(List<GridFilter> filters) {
		this.filters = filters;
	}

	public void addFilter(GridFilter filter) {
		filters = (filters != null) ? filters : new ArrayList<>();

		filters.add(filter);
	}

	public boolean hasFilters() {
		return (filters != null) && !filters.isEmpty();
	}

}
