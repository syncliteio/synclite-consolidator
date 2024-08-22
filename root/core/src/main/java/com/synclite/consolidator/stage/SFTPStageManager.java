package com.synclite.consolidator.stage;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;

public class SFTPStageManager extends DeviceStageManager {

	private Session masterSession;
	private ChannelSftp masterChannel;
	private ConcurrentHashMap<String, Session> containerSessions;    
	private ConcurrentHashMap<String, ChannelSftp> containerChannels;
	private String host;
	private String user;
	private Integer port;
	private String password;
	private String remoteStageDirectory;

	@Override
	public boolean objectExists(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		String container = objectPath.getParent().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container)) {
					connectContainer(container);
				}
				containerChannels.get(container).lstat(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"));
				return true;			
			} catch (Exception e) {
				if (e instanceof SftpException) {
					if (((SftpException) e).id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
						return false;
					}
				}
				tracer.error("Exception while checking if object exists in device stage for object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			} 
		}
		return false;		
	}

	@Override
	public long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
		String container = objectPath.getParent().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container)) {
					connectContainer(container);
				}

				//Delete the file at outputFile path		
				if (Files.exists(outputFile)) {
					Files.delete(outputFile);
				}

				ChannelSftp c = containerChannels.get(container);
				// Use lstat() to get file attributes
				SftpATTRS entry = c.lstat(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"));
				// Get modification time (mtime) and access time (atime)
				long publishTime = entry.getMTime() * 1000L; 

				// Download the file
				OutputStream outputStream = new FileOutputStream(outputFile.toString());
				c.get(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"), outputStream);
				outputStream.close();

				return publishTime;			
			} catch (Exception e) {
				if (e instanceof SftpException) {
					if (((SftpException) e).id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
						return 0;
					}
				} else {
					tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
					if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
						throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
					} catch (InterruptedException e1) {
						Thread.interrupted();
					}					
				}
			} 
		}
		return 0;
	}

	@Override
	public long doDownloadAndDecryptObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
		String container = objectPath.getParent().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container)) {
					connectContainer(container);
				}

				ChannelSftp c = containerChannels.get(container);

				InputStream fis = c.get(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"));      
				fileDownloader.decryptAndWriteFile(fis, outputFile);
				// Use lstat() to get file attributes
				SftpATTRS entry = c.lstat(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"));
				// Get modification time (mtime) and access time (atime)
				long publishTime = entry.getMTime() * 1000L; 
				return publishTime;			
			} catch (Exception e) {
				if (e instanceof SftpException) {
					if (((SftpException) e).id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
						return 0;
					}
				} else {
					tracer.error("Exception while downloading object from device stage, object : " + objectPath, e);				
					if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
						throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
					}
					try {
						Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
					} catch (InterruptedException e1) {
						Thread.interrupted();
					}					
				}
			} 
		}
		return 0;
	}

	@Override
	public void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException {
		String container = objectPath.getParent().toString();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container)) {
					connectContainer(container);
				}
				containerChannels.get(container).rm(remoteStageDirectory + "/" + objectPath.toString().replace("\\", "/"));
				return;
			} catch (Exception e) {
				if (e instanceof SftpException) {
					if (((SftpException) e).id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
						return;
					}
				}
				tracer.error("Exception while deleting object from device stage, object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
	}

	@Override
	public Path findObjectWithSuffix(Path container, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container.toString())) {
					connectContainer(container.toString());
				}
				// List files in the remote directory
				Vector<ChannelSftp.LsEntry> files = containerChannels.get(container.toString()).ls(remoteStageDirectory + "/" + container.toString().replace("\\", "/") + "/*" + suffix); 

				for (ChannelSftp.LsEntry file : files) {
					return Path.of(container.toString(), file.getFilename());
				}	            
			} catch (Exception e) {
				tracer.error("Exception while finding object with " + suffix + " from device stage in container : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return null;        
	}

	
	@Override
	public List<Path> findObjectsWithSuffixPrefix(Path container, String prefix, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException  {
		List<Path> objects = new ArrayList<Path>();
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container.toString())) {
					connectContainer(container.toString());
				}
				// List files in the remote directory
				Vector<ChannelSftp.LsEntry> files = containerChannels.get(container.toString()).ls(remoteStageDirectory + "/" + container.toString().replace("\\", "/") + "/*" + suffix); 

				for (ChannelSftp.LsEntry file : files) {
					if (file.getFilename().startsWith(prefix)) {
						objects.add(Path.of(container.toString(), file.getFilename()));
					}
				}	            
			} catch (Exception e) {
				tracer.error("Exception while finding object with prefix : " + prefix  + " and suffix : " + suffix + " from device stage in container : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return null;        
	}

	@Override
	public List<Path> listObjects(Path container) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container.toString())) {
					connectContainer(container.toString());
				}
				ChannelSftp c = containerChannels.get(container.toString()); 

				c.cd(container.toString());

				Vector<ChannelSftp.LsEntry> entries = c.ls(remoteStageDirectory + "/" + container.toString().replace("\\", "/"));

				List<Path> objects = new ArrayList<Path>();
				for (ChannelSftp.LsEntry entry : entries) {
					if (!entry.getAttrs().isDir() && !entry.getFilename().equals(".") && !entry.getFilename().equals("..")) {
						objects.add(Path.of(container.toString(), entry.getFilename()));
					}
				}
				return objects;
			} catch (Exception e) {
				tracer.error("Exception while listing objects in container " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return Collections.EMPTY_LIST;
	}

	@Override
	public boolean containerExists(Path containerPath, SyncLiteObjectType onjectType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isConnected()) {
					connect();
				}
				masterChannel.lstat(remoteStageDirectory + "/" + containerPath.toString().replace("\\", "/"));
				return true;			
			} catch (Exception e) {
				if (e instanceof SftpException) {
					if (((SftpException) e).id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
						return false;
					}
				}
				tracer.error("Exception while checking if container exists in device stage for object : " + containerPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			} 
		}
		return false;
	}

	@Override
	public List<Path> listContainers(Path startFrom, SyncLiteObjectType objectType) throws SyncLiteStageException {		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isConnected()) {
					connect();
				}
				masterChannel.cd(this.remoteStageDirectory.replace("\\", "/"));

				Vector<ChannelSftp.LsEntry> entries = masterChannel.ls(this.remoteStageDirectory.toString().replace("\\", "/"));

				List<Path> containers= new ArrayList<Path>();
				for (ChannelSftp.LsEntry entry : entries) {
					if (entry.getAttrs().isDir() && !entry.getFilename().equals(".") && !entry.getFilename().equals("..")) {
						containers.add(Path.of(entry.getFilename()));
					}
				}
				return containers;
			} catch (Exception e) {
				tracer.error("Exception while listing containers in device stage ", e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
		return Collections.EMPTY_LIST;
	}

	private void initializeStage(String remotePath) throws SyncLiteStageException {
		try {
			this.host = ConfLoader.getInstance().getStageSFTPHost();
			this.port = ConfLoader.getInstance().getStageSFTPPort();
			this.user = ConfLoader.getInstance().getStageSFTPUser();
			this.password = ConfLoader.getInstance().getStageSFTPPassword();
			this.remoteStageDirectory = ConfLoader.getInstance().getStageSFTPDataDirectory();
			connect();

			//Check if the specified remote data stage dir exists.
			masterChannel.lstat(remoteStageDirectory.toString());

			containerSessions = new ConcurrentHashMap<String, Session>();
			containerChannels = new ConcurrentHashMap<String, ChannelSftp>();

		} catch (Exception e) {
			tracer.error("Failed to connect to stage SFTP server", e);
			throw new SyncLiteStageException("Device stage unreachable. Failed to connect to specified SFTP server.", e);
		}	
		
	}
	
	@Override
	public void initializeDataStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initializeStage(ConfLoader.getInstance().getStageSFTPDataDirectory());		
	}
	
	@Override
	public void initializeCommandStage(Logger tracer) throws SyncLiteStageException {
		this.tracer = tracer;
		initializeStage(ConfLoader.getInstance().getStageSFTPCommandDirectory());		
	}

	@Override
	public void deleteContainer(Path container) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isConnected()) {
					connect();
				}
				//Delete all objects first
				List<Path> objects = listObjects(container);
				for (Path obj : objects) {
					deleteObject(obj, null);
				}
				masterChannel.rmdir(container.toString());
				return;
			} catch (Exception e) {
				tracer.error("Exception while deleting container from device stage : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
	}

	private final void connect() {
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			//jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
			masterSession=jsch.getSession(user, host, port);
			//session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
			masterSession.setPassword(password);            
			masterSession.setConfig(config);
			masterSession.setTimeout(Integer.MAX_VALUE);
			masterSession.connect();
			masterChannel = (ChannelSftp) masterSession.openChannel("sftp");
			masterChannel.connect(Integer.MAX_VALUE);
		} catch (Exception e) {
			tracer.error("SFTP Connection failed with exception : ", e);
		}
	}

	private final void connectContainer(String container) {
		try {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			JSch jsch = new JSch();
			//jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
			Session s=jsch.getSession(user, host, port);
			//session.setConfig("PreferredAuthentications", "publickey, keyboard-interactive,password");
			s.setPassword(password);            
			s.setConfig(config);
			s.setTimeout(Integer.MAX_VALUE);
			s.connect();
			ChannelSftp c = (ChannelSftp) s.openChannel("sftp");
			c.connect(Integer.MAX_VALUE);
			containerSessions.put(container, s);
			containerChannels.put(container, c);            
		} catch (Exception e) {
			tracer.error("SFTP Connection failed with exception while connecting for container : " + container, e);
		}
	}

	private final boolean isConnected() {
		if (masterSession == null) {
			return false;
		}
		if(masterChannel == null) {
			return false;
		}
		if (!masterSession.isConnected()) {
			return false;
		}
		if (!masterChannel.isConnected()) {
			return false;
		}
		if(masterChannel.isClosed()) {
			return false;
		}
		return true;
	}

	private final boolean isContainerConnected(String containerName) {
		Session s = containerSessions.get(containerName);
		ChannelSftp c = containerChannels.get(containerName);

		if (s == null) {
			return false;
		}
		if(c == null) {
			return false;
		}
		if (!s.isConnected()) {
			return false;
		}
		if (!c.isConnected()) {
			return false;
		}
		if(c.isClosed()) {
			return false;
		}
		return true;
	}

	@Override
	public void createContainer(Path container, SyncLiteObjectType objType) throws SyncLiteStageException {
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isConnected()) {
					connect();
				}

				if (containerExists(container, objType)) {
					return;
				}

				String remoteContainerPath = remoteStageDirectory + "/" + container.toString().replace("\\", "/");
				masterChannel.cd(remoteStageDirectory);
				masterChannel.mkdir(remoteContainerPath);        	
				return;
			} catch (Exception e) {
				tracer.error("Exception while deleting container from device stage : " + container, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}
			}
		}
	}

	@Override
	public void uploadObject(Path container, Path objectPath, SyncLiteObjectType objType)
			throws SyncLiteStageException {
		
		for (long i = 0; i < ConfLoader.getInstance().getStageOperRetryCount(); ++i) {
			try {
				if (!isContainerConnected(container.toString())) {
					connectContainer(container.toString());
				}

				String remoteObjectPath = remoteStageDirectory + "/" +  container.toString().replace("\\", "/") + objectPath.getFileName().toString().replace("\\", "/");
				ChannelSftp c = containerChannels.get(container.toString());

				c.put(objectPath.toString(), remoteObjectPath);
			} catch (Exception e) {
				tracer.error("Exception while uploading object to device stage, object : " + objectPath, e);				
				if (i == (ConfLoader.getInstance().getStageOperRetryCount()-1)) {
					throw new SyncLiteStageException("Stage operation failed after all retry attempts : ", e);
				}
				try {
					Thread.sleep(ConfLoader.getInstance().getStageOperRetryIntervalMs());
				} catch (InterruptedException e1) {
					Thread.interrupted();
				}					

			} 
		}
	}

}
