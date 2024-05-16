package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.model.DriveListRequest;
import com.spinsys.mdaca.storage.explorer.model.exception.AuthorizationException;
import com.spinsys.mdaca.storage.explorer.model.http.ActionAuditResponse;
import com.spinsys.mdaca.storage.explorer.model.http.AuditStatisticsSpec;
import com.spinsys.mdaca.storage.explorer.model.http.GridFilter;
import com.spinsys.mdaca.storage.explorer.model.http.GridStateSpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveProperty;
import com.spinsys.mdaca.storage.explorer.persistence.DriveSecurityRule;
import com.spinsys.mdaca.storage.explorer.persistence.DriveUser;
import com.spinsys.mdaca.storage.explorer.persistence.TableUtils;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.*;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.PENDING;

@Path("admin")
public class AdminService extends BaseService {

	/** When this service started. */
	static final Date SERVICE_START_TIME = new Date();


	@PersistenceContext(unitName = TableUtils.STOREXP_PERSISTENT_UNIT)
	protected EntityManager entityManager;

	public AdminService() {

	}

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.AdminService");

	@OPTIONS
	@Path("drives")
	public Response getDrivesOPTIONS() {
		return populateSuccessResponse();
	}

	@PUT
	@Path("drives")
	public Response updateDrives(@Context HttpServletRequest request, final DriveListRequest driveListRequest) {
		//try and log each drive creation separately; any way to optimize this?
		for (Drive drive : driveListRequest.getDrives()) {

			List<ActionAudit> audits = new ArrayList<>();

			audits.add(this.auditAction("updateDrive", drive.toString(), drive.getDriveId(), PENDING));

			try {
				List<DriveProperty> providerProperties = drive.getProviderProperties();
				for (DriveProperty prop : providerProperties) {
					prop.setDrive(drive);
					audits.add(this.auditAction("updateDriveProperty", prop.toString(), drive.getDriveId(), PENDING));
				}

				List<DriveSecurityRule> securityRules = drive.getSecurityRules();
				for (DriveSecurityRule rule : securityRules) {
					rule.setDrive(drive);
					audits.add(this.auditAction("updateDriveSecurityRule", rule.toString(), drive.getDriveId(), PENDING));
				}

				List<DriveUser> users = drive.getUsers();
				for (DriveUser user : users) {
					user.setDrive(drive);
					audits.add(this.auditAction("updateDriveUser", user.toString(), drive.getDriveId(), PENDING));
				}

				String userName = request.getUserPrincipal().getName();

				if (!getUserRole().equals(ADMIN) && !getDrivesByUserName(userName).contains(drive)) {
					throw new AuthorizationException("User does not have permissions to this drive");
				}

				utx.begin();
				entityManager.merge(drive);
				entityManager.flush();
				utx.commit();

				for (ActionAudit action : audits) {
					this.recordSuccess(action);
				}

			} catch (AuthorizationException e) {
				for (ActionAudit action : audits) {
					recordUnauthorized(action);
				}
			} catch (Exception e) {

				for (ActionAudit action : audits) {
					this.recordException(action, e);
				}
				logger.log(Level.WARNING, e.getMessage(), e);
			}
		}
		return populateSuccessResponse();
	}

	@POST
	@Path("drives")
	public Response createDrives(@Context HttpServletRequest request, final DriveListRequest driveListRequest) {
		List<ActionAudit> audits = new ArrayList<>();
		//try and log each drive creation separately; any way to optimize this?
		for (Drive drive : driveListRequest.getDrives()) {
			ActionAudit action = this.auditAction("createDrive", drive.toString(), 0, PENDING);
			audits.add(action);

			if (!getUserRole().equals(ADMIN)) {
				recordUnauthorized(action);
				break;
			}

			try {
				utx.begin();
				entityManager.persist(drive);
				entityManager.flush();
				utx.commit();

				action.setDrive(drive);
				recordSuccess(action);

			} catch (Exception e) {
				recordException(action, e);
				logger.log(Level.WARNING, e.getMessage(), e);
			}
		}

		return populateSuccessResponse(audits);
	}

	@POST
	@Path("audit")
    public Response getAudit(GridStateSpec spec, @Context HttpServletRequest request) throws ParseException, IOException {
		ActionAuditResponse resp = new ActionAuditResponse();

		CriteriaBuilder qb = entityManager.getCriteriaBuilder();
		CriteriaQuery<ActionAudit> cq = qb.createQuery(ActionAudit.class);
		Root<ActionAudit> root = cq.from(ActionAudit.class);
		cq.select(root);

		String sortField = spec.getSortField().equals("drive.driveId") ? "drive" : spec.getSortField();
		if(spec.getSortDir() == null || spec.getSortDir().equals("asc")) {
			cq.orderBy(qb.asc(root.get(sortField)));
		} else {
			cq.orderBy(qb.desc(root.get(sortField)));
		}

		if (spec.hasFilters()) {
			applyGridFilters(qb, cq, spec.getFilters(), root);
		}

		TypedQuery<ActionAudit> query = entityManager.createQuery(cq);
		TypedQuery<ActionAudit> queryWithParameter = query.setMaxResults(spec.getEndRow() - spec.getStartRow()).setFirstResult(spec.getStartRow());
		List<ActionAudit> result = queryWithParameter.getResultList();

		result.stream()
				.filter(actionAudit -> actionAudit.getDrive() != null)
				.forEach(actionAudit -> actionAudit.getDrive().voidMappedClasses());

		resp.setAudits(result);

		CriteriaBuilder qbCount = entityManager.getCriteriaBuilder();
		CriteriaQuery<Long> cqCount = qbCount.createQuery(Long.class);
		cqCount.select(qbCount.count(cqCount.from(ActionAudit.class)));

		if(spec.hasFilters()) {
			applyGridFilters(qbCount, cqCount, spec.getFilters(), root);
		}

		resp.setTotal(entityManager.createQuery(cqCount).getSingleResult());

		return populateSuccessResponse(resp);
	}

	void applyGridFilters(CriteriaBuilder qb, CriteriaQuery cq, List<GridFilter> filters, Root root) throws ParseException, IOException {

		List<Predicate> predList = new LinkedList<Predicate>();

		for(GridFilter filt : filters) {


			String field = filt.getField();
			Object value = filt.getValue();

			javax.persistence.criteria.Path path;

			if (field.equals("drive.driveId")) {
				path = root.get("drive");

				Subquery<Drive> subquery = cq.subquery(Drive.class);
				Root<Drive> fromDrive = subquery.from(Drive.class);

				GridFilter driveIdFilter = new GridFilter();
				driveIdFilter.setField("driveId");
				driveIdFilter.setOperator(filt.getOperator());
				Expression<String> driveIdPath = fromDrive.get("driveId").as(String.class);
				value = (value != null) ? value : "";
				subquery.select(fromDrive)
						.where(getPredicate(qb, driveIdFilter, value, driveIdPath));
//				getPredicate(qb, filt, value, driveIdPath)
				predList.add(qb.in(path).value(subquery));
			} else if (field.equals("created")) {
				path = root.get(field);

				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date targetDate = simpleDateFormat.parse(value.toString());
				predList.add(qb.greaterThanOrEqualTo(root.get(field), targetDate));

				Calendar cal = Calendar.getInstance();
				cal.setTime(targetDate);
				cal.add(Calendar.DATE, 1);
				predList.add(qb.lessThan(path, cal.getTime()));
			} else {
				path = root.get(field);

				predList.add(getPredicate(qb, filt, value, path));
			}

		}

		Predicate[] predArray = new Predicate[predList.size()];
		predList.toArray(predArray);
		cq.where(predArray);
	}

	private Predicate getPredicate(CriteriaBuilder qb, GridFilter filt, Object value, Expression exp) throws IOException {
		switch(filt.getOperator()) {
			case "contains":
				return qb.like(exp, "%" + value + "%");
			case "eq":
				return qb.equal(exp, value);
			case "doesnotcontain":
				return qb.not(qb.like(exp, "%" + value + "%"));
			case "neq":
				return qb.not(qb.equal(exp, value));
			case "startswith":
				return qb.like(exp, value + "%");
			case "endswith":
				return qb.like(exp, "%" + value);
			case "isnull":
				return qb.isNull(exp);
			case "isnotnull":
				return qb.isNotNull(exp);
			case "isempty":
				return qb.equal(exp, "");
			case "isnotempty":
				return qb.not(qb.equal(exp, ""));
			default:
				throw new IOException("Unhandled operator type: " + filt.getOperator());
		}
	}

	private List<Drive> getDrivesByUserName(String userName) {
		TypedQuery<Drive> query;
		query = getEntityManager().createQuery("select distinct du.drive from DriveUser du " +
				"where (du.userName = :userName)", Drive.class)
				.setParameter("userName", userName);
		List<Drive> result = query.getResultList();
		return result;
	}

	@POST
	@Path("testConnection")
	public Response testConnection(final Drive drive, @Context HttpServletRequest request,
			@Context HttpServletResponse resp) {
		Response response;
		ActionAudit action = auditAction("testConnection", drive.toString(), drive.getDriveId(), PENDING);

		try {
			StorageProvider provider =
			        StorageProviderFactory.getProvider(drive.getDriveType(), request);
			boolean testConnection = provider.testConnection(drive);

            recordSuccess(action);
			response = populateSuccessResponse(testConnection);
		} catch (Exception e) {
            recordException(action, e);
			response = populateSuccessResponse(false);
		}
		return response;
	}

	@OPTIONS
	@Path("auditStatistics")
	public Response auditStatisticsOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("auditStatistics")
	public Response auditStatistics(AuditStatisticsSpec spec, @Context HttpServletRequest request)
	{
		Date fromDate = (spec.getFromDate() != null)
				? spec.getFromDate()
				: SERVICE_START_TIME;
		String status = spec.getStatus(); // e.g., pending
		Map<String, Number> stateQty = ActionAudit.getActionStatistics(status, fromDate, entityManager);
		return populateSuccessResponse(stateQty);
	}
	

}
