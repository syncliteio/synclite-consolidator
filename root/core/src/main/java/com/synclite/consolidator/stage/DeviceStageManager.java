package com.synclite.consolidator.stage;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.log4j.Logger;

import com.synclite.consolidator.exception.SyncLiteStageException;
import com.synclite.consolidator.global.ConfLoader;

public abstract class DeviceStageManager {

	protected abstract class FileDownloader {
		protected abstract long downloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException;
		protected abstract void decryptAndWriteFile(InputStream fis, Path outputFile) throws SyncLiteStageException;
	}

	protected class PlainOutputDownloader extends FileDownloader {
		@Override
		protected long downloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
			return doDownloadObject(objectPath, outputFile, objType);

		}

		@Override
		protected void decryptAndWriteFile(InputStream fis, Path outputFile) throws SyncLiteStageException {
			throw new IllegalAccessError("No decryption possible in PlainFileDownloader");			
		}
	}

	protected class EncryptedFileDownloader extends FileDownloader{
		private static final int ENCRYPTION_BLOCK_SIZE = 2048;

		@Override
		protected long downloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
			return doDownloadAndDecryptObject(objectPath, outputFile, objType);
		}


		@Override
		protected void decryptAndWriteFile(InputStream fis, Path outputFile) throws SyncLiteStageException {
			//Delete and recreated already existing outputFile				
			try {
				if (Files.exists(outputFile)) {
					Files.delete(outputFile);
				}
				Files.createFile(outputFile);
			} catch (IOException e) {
				throw new SyncLiteStageException("Failed to delete/re-create file at path : " + outputFile);
			}

			PrivateKey privateKey;
			try {
				PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Files.readAllBytes(ConfLoader.getInstance().getDeviceDecryptionKeyFile()));
				KeyFactory keyFactory = KeyFactory.getInstance("RSA");
				privateKey = keyFactory.generatePrivate(privateKeySpec);
			} catch (IOException | InvalidKeySpecException | NoSuchAlgorithmException e) {
				throw new SyncLiteStageException("Failed to load device decryption key from the specified file", e);
			}

			try (DataInputStream dis = new DataInputStream(fis)) {
				int encryptedKeySize = dis.readInt();
				byte[] encryptedKey = dis.readNBytes(encryptedKeySize);

				byte[] decryptedKey;
				try {
					Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
					cipher.init(Cipher.DECRYPT_MODE, privateKey);
					decryptedKey = cipher.doFinal(encryptedKey);
				} catch (NoSuchPaddingException | InvalidKeyException | IllegalBlockSizeException | BadPaddingException | NoSuchAlgorithmException e) {
					throw new SyncLiteStageException("Failed to decrypt the encrypted key read from the object", e);
				}

				int ivSize = dis.readInt();
				byte[] iv = dis.readNBytes(ivSize);
				IvParameterSpec ivParams = new IvParameterSpec(iv);
				byte[] buffer = new byte[ENCRYPTION_BLOCK_SIZE];
				int bytesRead;

				Cipher cipher;
				try {
					cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
					SecretKeySpec secretKey = new SecretKeySpec(decryptedKey, "AES");
					cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParams);
				} catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
					throw new SyncLiteStageException("Failed to create a cipher out of decrypted encryption key");
				}

				try (FileOutputStream fos = new FileOutputStream(outputFile.toString())) {
					while ((bytesRead = dis.read(buffer)) != -1) {
						byte[] decrypted = cipher.update(buffer, 0, bytesRead);
						fos.write(decrypted);
					}
					byte[] finalBytes = cipher.doFinal();
					if (finalBytes.length > 0) {
						fos.write(finalBytes);
					}
					fos.flush();
				} catch (IOException | IllegalBlockSizeException | BadPaddingException e) {
					throw new SyncLiteStageException("Failed to read data, encrypt and write out to output file", e);
				}
			} catch (IOException e) {
				throw new SyncLiteStageException("Failed to open and read contents of input stream ", e);
			}
		}
	}

	protected Logger tracer;
	protected FileDownloader fileDownloader; 

	protected DeviceStageManager() {
		if (ConfLoader.getInstance().getDeviceEncryptionEnabled()) {
			this.fileDownloader = new EncryptedFileDownloader();
		} else {
			this.fileDownloader = new PlainOutputDownloader();
		}
	}

	private static final class InstanceHolder {
		private static DeviceStageManager DATA_STAGE_INSTANCE = instantiate();
		private static DeviceStageManager COMMAND_STAGE_INSTANCE = instantiate();
		
		private static DeviceStageManager instantiate() {
			switch(ConfLoader.getInstance().getDeviceStageType()) {
			case FS:
			case LOCAL_SFTP:
			case MS_ONEDRIVE:
			case GOOGLE_DRIVE:
				return new FSDeviceStageManager();
			case REMOTE_SFTP:
				return new SFTPStageManager();
			case LOCAL_MINIO:
				return new LocalMinioStageManager();
			case REMOTE_MINIO:
				return new RemoteMinioStageManager();
			case KAFKA:
				return new KafkaStageManager();
			case S3:
				return new S3StageManager();
			default:
				throw new RuntimeException("Unsupported Device Stage Type :" + ConfLoader.getInstance().getDeviceStageType());			
			}
		}
	}

	public static DeviceStageManager getDataStageManagerInstance() {
		return InstanceHolder.DATA_STAGE_INSTANCE;
	}

	public static DeviceStageManager getCommandStageManagerInstance() {
		return InstanceHolder.COMMAND_STAGE_INSTANCE;
	}
	

	public long downloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException {
		return fileDownloader.downloadObject(objectPath, outputFile, objType);
	}

	@Deprecated
	public abstract boolean objectExists(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract long doDownloadObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract long doDownloadAndDecryptObject(Path objectPath, Path outputFile, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract void deleteObject(Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract Path findObjectWithSuffix(Path container, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException ;
	public abstract List<Path> findObjectsWithSuffixPrefix(Path container, String prefix, String suffix, SyncLiteObjectType objType) throws SyncLiteStageException ;	
	public abstract List<Path> listObjects(Path container) throws SyncLiteStageException;
	public abstract boolean containerExists(Path containerPath, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract List<Path> listContainers(Path startFrom, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract void initializeDataStage(Logger globalTracer) throws SyncLiteStageException;
	public abstract void initializeCommandStage(Logger globalTracer) throws SyncLiteStageException;
	public abstract void deleteContainer(Path container) throws SyncLiteStageException;
	public abstract void createContainer(Path container, SyncLiteObjectType objType) throws SyncLiteStageException;
	public abstract void uploadObject(Path container, Path objectPath, SyncLiteObjectType objType) throws SyncLiteStageException;
	
}
