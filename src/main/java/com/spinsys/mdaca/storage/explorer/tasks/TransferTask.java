package com.spinsys.mdaca.storage.explorer.tasks;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.GUI_SEP;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.sameSourceAndDestination;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Delete;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Read;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.PENDING;

import java.io.IOException;
import java.security.Principal;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.UserTransaction;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ArchiveException;
import com.spinsys.mdaca.storage.explorer.model.exception.AuthorizationException;
import com.spinsys.mdaca.storage.explorer.model.exception.DisplayableException;
import com.spinsys.mdaca.storage.explorer.model.exception.DriveItemExistsException;
import com.spinsys.mdaca.storage.explorer.model.exception.MissingInputException;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;
import com.spinsys.mdaca.storage.explorer.rest.BaseService;
import com.spinsys.mdaca.storage.explorer.rest.DriveService;

import net.sourceforge.spnego.SpnegoPrincipal;

public class TransferTask  implements Callable<Boolean> {
	
	public TransferTask(List<TransferSpec> specs, HttpServletRequest request, EntityManager entityManager, UserTransaction utx, SpnegoPrincipal principal, List<String> roles, String username, String ipAddress) {
		
		if(specs.size() > 0) {
			_resolution = specs.get(0).getResolution();
		}
		
		_specs = specs;
		_request = request;
		_entityManager = entityManager;
		_utx = utx;
		_principal = principal;
		_roles = roles;
		_username = username;
		_ipAddress = ipAddress;
		_action = BaseService.auditAction(specs.get(0).isRemoveSource() ? "batch_move" : "batch_copy", "task", specs.get(0).getSourceDriveId(), PENDING,
				PathProcessor.getParentFolder(specs.get(0).getDestPath()), specs.get(0).getDestDriveId(), null, null, username, ipAddress);

		BaseService.request = _request;
		BaseService.utx = _utx;
		BaseService.entityManager = _entityManager;
		
		_action = _entityManager.find(ActionAudit.class, _action.getActionAuditId());
		_action.setTotalBytes(new Long(_specs.size()));
		BaseService.updateAction(_action);
	}

	String _resolution;
	List<TransferSpec> _specs;
	HttpServletRequest _request;
	HttpServletResponse _resp;
	EntityManager _entityManager;
	UserTransaction _utx;
	SpnegoPrincipal _principal;
	List<String> _roles;
	ActionAudit _action;
	String _username;
	String _ipAddress;

    static Logger logger = Logger.getLogger("com.spinsys.mdaca.storage.explorer.tasks.TransferTask");

    public Boolean call() {

    	try {
	        Response response = null;
	        
			BaseService.request = _request;
			BaseService.utx = _utx;
			BaseService.entityManager = _entityManager;
			

			_action = _entityManager.find(ActionAudit.class, _action.getActionAuditId());
			
			int count = 0;
			
			for(TransferSpec spec : _specs) {
				count += getItemCount(spec, _request, _principal, _roles, _username, _ipAddress, _action);
			}

			if(count != _action.getTotalBytes().intValue()) {
				_action.setTotalBytes(new Long(count));
				BaseService.updateAction(_action);
			}

			for(TransferSpec spec : _specs) {
				
				_action = _entityManager.find(ActionAudit.class, _action.getActionAuditId());
				
				if(_action.getStatus().compareTo("A") == 0) {
					_action.setStatus("C");
					BaseService.updateAction(_action);
					return false;
				}
	
				response = transferLogic(spec, _request, _principal, _roles, _username, _ipAddress, _action, _resolution);
					
				if(response.getStatus() != 200) {
					_action = _entityManager.find(ActionAudit.class, _action.getActionAuditId());
					if(_action.getStatus().compareTo("C") != 0) {
			    		logger.fatal("Tranfser Task Failed");
						BaseService.recordException(_action, new Exception("transferLogic failed"), _action.getBytesTransferred());
					}
					return false;
				}
				
				_action = _entityManager.find(ActionAudit.class, _action.getActionAuditId());
				_action.setBytesTransferred(_action.getBytesTransferred()+1);
				int percent = (int) Math.round(((double)_action.getBytesTransferred() / (_action.getTotalBytes() == null ? 0 : _action.getTotalBytes())) * 100);
				_action.setPercentComplete(percent);
				BaseService.updateAction(_action);
			}

			BaseService.recordSuccess(_action, _specs.size());
			return true;
    	} catch(Exception ex) {
    		logger.fatal("Tranfser Task Failed", ex);
			BaseService.recordException(_action, ex, _action.getBytesTransferred());
    	}
		return false;
    }
    
    public static int getItemCount(TransferSpec spec, HttpServletRequest request, SpnegoPrincipal principal, List<String> roles, String username, String ipAddress, ActionAudit _action) throws IOException {
    	boolean isMove = spec.isRemoveSource();

		int sourceDriveId = spec.getSourceDriveId();
		String destPath = spec.getDestPath();
		String sourcePath = spec.getSourcePath();

		Drive sourceDrive = DriveService.getDrive(sourceDriveId);
		if (sourceDrive == null) {
			throw new MissingInputException("No source drive provided.");
		}

		int destDriveId = spec.getDestDriveId();
		Drive destDrive = DriveService.getDrive(destDriveId);
		if (destDrive == null) {
			throw new MissingInputException("Unrecognized or missing destination drive.");
		}

		if (destPath == null) {
			throw new MissingInputException("No destination path provided.");
		}

		if (sourcePath == null) {
			throw new MissingInputException("No source path provided.");
		}

		if (sameSourceAndDestination(sourceDriveId, sourcePath, destDriveId, destPath)) {
			throw new DriveItemExistsException("Must pick a new destination for path: " + sourcePath);
		}

		// Don't allow a directory to be moved beneath itself.
		if (isMove && sourceDriveId == destDriveId && sourcePath.endsWith(GUI_SEP) && destPath.startsWith(sourcePath)) {
			throw new DisplayableException(sourcePath + " cannot be moved beneath itself to " + destPath);
		}
		
		DriveType sourceDriveType = sourceDrive.getDriveType();
		StorageProvider sourceProvider = StorageProviderFactory.getProvider(sourceDriveType, principal);

		if (sourceProvider.isDirectory(sourceDrive, sourcePath)) {


			// Transfer the directory and everything under the directory
			List<DriveItem> driveItems = sourceProvider.findAllInPath(sourceDrive, sourcePath);
			

			//true IFF the list of drive items contains an item that is archived
			boolean hasArchivedFile = driveItems.stream().anyMatch(DriveItem::isRestoreRequired);

			if (hasArchivedFile) {
				throw new ArchiveException("Cannot transfer file(s) because one or more of the items are archived");
			}

			return driveItems.size();

		} else { 
			return 1;
		}
    }
    
    public static Response transferLogic(TransferSpec spec, HttpServletRequest request, SpnegoPrincipal principal, List<String> roles, String username, String ipAddress, ActionAudit _action, String _resolution) {
        Response response = null;
		boolean isMove = spec.isRemoveSource();

		int sourceDriveId = spec.getSourceDriveId();
		String destPath = spec.getDestPath();
		String sourcePath = spec.getSourcePath();
		ActionAudit action = BaseService.auditAction(isMove ? "move" : "copy", sourcePath, sourceDriveId, PENDING,
				destPath, spec.getDestDriveId(), null, null, username, ipAddress);

		long bytesTransferred = 0;

		try {
			BaseService.logRequest(request, spec);
			Drive sourceDrive = DriveService.getDrive(sourceDriveId);
			if (sourceDrive == null) {
				throw new MissingInputException("No source drive provided.");
			}

			int destDriveId = spec.getDestDriveId();
			Drive destDrive = DriveService.getDrive(destDriveId);
			if (destDrive == null) {
				throw new MissingInputException("Unrecognized or missing destination drive.");
			}

			if (destPath == null) {
				throw new MissingInputException("No destination path provided.");
			}

			if (sourcePath == null) {
				throw new MissingInputException("No source path provided.");
			}

			if (sameSourceAndDestination(sourceDriveId, sourcePath, destDriveId, destPath)) {
				throw new DriveItemExistsException("Must pick a new destination for path: " + sourcePath);
			}

			// Don't allow a directory to be moved beneath itself.
			if (isMove && sourceDriveId == destDriveId && sourcePath.endsWith(GUI_SEP) && destPath.startsWith(sourcePath)) {
				throw new DisplayableException(sourcePath + " cannot be moved beneath itself to " + destPath);
			}

			DriveType destDriveType = destDrive.getDriveType();
			DriveType sourceDriveType = sourceDrive.getDriveType();
			StorageProvider sourceProvider = StorageProviderFactory.getProvider(sourceDriveType, principal);
			StorageProvider destProvider = StorageProviderFactory.getProvider(destDriveType, principal);

			//moving a file involves deleting the source, so require "delete" permissions if this transfer is a move
			AccessLevel minimumSourceAccessLevel = isMove ? Delete : Read;

			if (!DriveService.assertDriveItemAccess(sourcePath, sourceDrive, minimumSourceAccessLevel, roles, username) ||
					!DriveService.assertDriveItemAccess(destPath, destDrive, AccessLevel.Create, roles, username)) {
				throw new AuthorizationException(
						"User does not have permissions to transfer " +
								sourcePath + " on " + sourceDrive + " to " +
								destPath + " on " + destDrive);
			}

			if (sourceProvider.isDirectory(sourceDrive, sourcePath)) {
				
				if (destProvider.exists(destDrive, destPath)) {
					
					if(_resolution.compareTo("cancel") == 0) {
						throw new DriveItemExistsException("Unable to transfer " +
							sourcePath + " on " + sourceDrive + " to " +
							destPath + " on " + destDrive + " because it already exists.");
					}
				} else {
					destProvider.mkdir(destDrive, destPath);
				}

				destProvider.mkdir(destDrive, destPath);

				// Transfer the directory and everything under the directory
				List<DriveItem> driveItems = sourceProvider.findAllInPath(sourceDrive, sourcePath);

				//true IFF the list of drive items contains an item that is archived
				boolean hasArchivedFile = driveItems.stream().anyMatch(DriveItem::isRestoreRequired);

				if (hasArchivedFile) {
					throw new ArchiveException("Cannot transfer file(s) because one or more of the items are archived");
				}

				TransferSpec fileTransferSpec = new TransferSpec();
				fileTransferSpec.setDestDriveId(destDrive.getDriveId());
				fileTransferSpec.setSourceDriveId(sourceDriveId);

				driveItems.sort(Comparator.comparing(DriveItem::getPath));

				// Transfer each file
				for (DriveItem item : driveItems) {

					_action = BaseService.entityManager.find(ActionAudit.class, _action.getActionAuditId());
					if(_action.getStatus().compareTo("A") == 0) {
						_action.setStatus("C");
						BaseService.updateAction(_action);
						return BaseService.populateResponseOnException(_action);
					}
			        
					String itemSourcePath = item.getPath();
					String itemDestPath = StringUtils.replaceOnce(itemSourcePath, sourcePath, destPath);

					fileTransferSpec.setDestPath(itemDestPath);
					fileTransferSpec.setSourcePath(itemSourcePath);

					if (item.isDirectory()) {
						if(destProvider.exists(destDrive, itemDestPath) == false) {
							destProvider.mkdir(destDrive, itemDestPath);
						}
					} else {
						DriveService.transferOneFile(item, fileTransferSpec, sourceDrive, destDrive, action, principal, roles, _action, username, _resolution);
					}

					bytesTransferred += item.getFileSize();

					_action = BaseService.entityManager.find(ActionAudit.class, _action.getActionAuditId());
					_action.setBytesTransferred(_action.getBytesTransferred() + 1);
					int percent = (int) Math.round(((double)_action.getBytesTransferred() / _action.getTotalBytes()) * 100);
					_action.setPercentComplete(percent);
					BaseService.updateAction(_action);
				} // for

				if (isMove) {
					sourceProvider.delete(sourceDrive, sourcePath);
				}

			} else { // transfer one file
				DriveItem driveItem = sourceProvider.getDriveItem(sourceDrive, sourcePath);
				DriveService.transferOneFile(driveItem, spec, sourceDrive, destDrive, action, principal, roles, _action, username, _resolution);
			}

			BaseService.recordSuccess(action, bytesTransferred);
			response = BaseService.populateSuccessResponse(action);
		} catch (AuthorizationException e) {
			logger.log(Level.WARN, e.getMessage(), e);
			BaseService.recordUnauthorized(action, e, bytesTransferred);
			response = BaseService.populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARN, e.getMessage(), e);
			BaseService.recordException(action, e, bytesTransferred);
			response = BaseService.populateResponseOnException(action);
		}
		return response;
    }
}
