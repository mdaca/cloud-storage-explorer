package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.List;

import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;

public class ActionAuditResponse {
	private List<ActionAudit> audits;
	private long total;

	public ActionAuditResponse() {
	}

	public ActionAuditResponse(List<ActionAudit> audits) {
		setAudits(audits);
	}

	public long getTotal() {
		return total;
	}
	public void setTotal(long total) {
		this.total = total;
	}
	public List<ActionAudit> getAudits() {
		return audits;
	}

	public void setAudits(List<ActionAudit> audits) {
		this.audits = audits;

		clearUnallowedMessages();
	}

	private void clearUnallowedMessages() {
		if (audits == null) {
			return;
		}

		audits.forEach(actionAudit -> {
			actionAudit.setStackTrace(null);

			if (!actionAudit.isUseMessage()) {
				actionAudit.setMessage(null);
			}
		});
	}

	@Override
	public String toString() {
		return (audits != null && audits.size() > 0) ?
				audits.get(0).getMessage() :
				super.toString();
	}

}
