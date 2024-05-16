package com.spinsys.mdaca.storage.explorer.rest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.junit.jupiter.api.Disabled;
import com.spinsys.mdaca.storage.explorer.persistence.TableUtils;

class BaseServiceIT {

	@PersistenceContext(unitName = TableUtils.STOREXP_PERSISTENT_UNIT)
	protected EntityManager entityManager;

	@Disabled
	void testHibernate() {
		EntityManagerFactory emf = Persistence.createEntityManagerFactory(TableUtils.STOREXP_PERSISTENT_UNIT);
		entityManager = emf.createEntityManager();
//		BaseService service = new BaseService();
		Query query = entityManager.createQuery("select a from ActionAudit a");
		List<?> resultList = query.getResultList();
		assertNotNull(resultList);
	}

}
