package com.synclite.consolidator.global;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import com.synclite.consolidator.exception.SyncLiteException;

public final class LicenseVerifier {

	private static TreeMap<String, String> licenseProperties = new TreeMap<String, String>();

	static Map<String, String> getLicenseProperties() {
		return licenseProperties;
	}

	private static void loadLicenseProperties(Path filePath) throws SyncLiteException {
		//Mock license properties for OSS
		licenseProperties.put("max-num-devices", "UNLIMITED");
		licenseProperties.put("allowed-destinations", "ALL");
	}

	static final void validateLicense(Path filePath) throws SyncLiteException {		
		//Nothing to validate in OSS version.
		//
		loadLicenseProperties(filePath);
	}

}
