package com.synclite.consolidator.global;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.synclite.consolidator.exception.SyncLiteException;

public class SyncLiteAppLock {
	
	private Connection lock;
	public SyncLiteAppLock() {
	}
	
	public final void tryLock(Path workDir) throws SyncLiteException {		
		Path lockFile = workDir.resolve("synclite_consolidator.lock");
		String lockFileURL = "jdbc:sqlite:" + lockFile;
		try {
			this.lock = DriverManager.getConnection(lockFileURL);
			try (Statement stmt = this.lock.createStatement()) {
	            stmt.executeUpdate("PRAGMA locking_mode = EXCLUSIVE");
	            stmt.executeUpdate("BEGIN EXCLUSIVE");
			}
		} catch (Exception e) {
			throw new SyncLiteException("Failed to lock synclite consolidator lock " + lockFile + ". Please stop any other consolidator job running for the specified work-dir : " + workDir);
		}
	}
}
