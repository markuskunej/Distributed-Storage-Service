package shared;

import org.apache.log4j.Logger;

import shared.messages.KVMessage;
import shared.messages.IKVMessage.StatusType;
import java.io.InputStream;
import java.io.IOException;

// Encryption Imports
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.X509EncodedKeySpec;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;
import javax.management.RuntimeErrorException;

import java.security.GeneralSecurityException;

public class Crypto {
    private static Logger logger = Logger.getRootLogger();

    public static byte[] encrypt(byte[] data, PublicKey publicKey) {
        try {
            // Generate a random symmetric key
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(128);
            SecretKey secretKey = keyGen.generateKey();

            // Encrypt the message with the symmetric key
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            byte[] ivByte = new byte[cipher.getBlockSize()];
            IvParameterSpec ivParamsSpec = new IvParameterSpec(ivByte);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParamsSpec);
            byte[] encryptedData = cipher.doFinal(data);

            // Encrypt the symmetric key with the RSA public key
            Cipher rsaCipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            rsaCipher.init(Cipher.ENCRYPT_MODE, publicKey);
            byte[] encryptedKey = rsaCipher.doFinal(secretKey.getEncoded());

                        // Logs for demo
            Encoder encoder = Base64.getEncoder();
            logger.info("\n*SENT MESSAGE*");
            logger.info("AES Key:\t '" + encoder.encodeToString(secretKey.getEncoded()) + "'");
            logger.info("Encrypted AES Key:\t '" + encoder.encodeToString(encryptedKey) + "'");
            logger.info("Message:\t '" + new String(data).trim() + "'");
            logger.info("Encrypted Message:\t '" + encoder.encodeToString(encryptedData) + "'");

            // Combine the encrypted key and the encrypted message
            byte[] result = new byte[encryptedKey.length + encryptedData.length + Integer.BYTES];
            ByteBuffer bb = ByteBuffer.wrap(result);
            bb.putInt(encryptedKey.length);
            bb.put(encryptedKey);
            bb.put(encryptedData);
            return result;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] decrypt(ByteBuffer bb, PrivateKey privateKey) {
        try {
            //ByteBuffer bb = ByteBuffer.wrap(combinedData);
            int encryptedKeyLength = bb.getInt();
            
            byte[] encryptedKey = new byte[encryptedKeyLength];
            bb.get(encryptedKey);

            byte[] encryptedData = new byte[bb.remaining()];
            bb.get(encryptedData);

            // Decrypt the symmetric key with the RSA private key
            Cipher rsaCipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            rsaCipher.init(Cipher.DECRYPT_MODE, privateKey);
            byte[] decryptedKey = rsaCipher.doFinal(encryptedKey);
            SecretKey secretKey = new SecretKeySpec(decryptedKey, "AES");

            // Decrypt the message with the symmetric key
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            byte[] ivByte = new byte[cipher.getBlockSize()];
            IvParameterSpec ivParamsSpec = new IvParameterSpec(ivByte);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParamsSpec);
            byte[] decryptedData = cipher.doFinal(encryptedData);

            // Logs for demo
            Encoder encoder = Base64.getEncoder();
            logger.info("\n*RECEIVED MESSAGE*");
            logger.info("Encrypted AES Key:\t '" + encoder.encodeToString(encryptedKey) + "'");
            logger.info("Decrypted AES Key:\t '" + encoder.encodeToString(decryptedKey) + "'");
            logger.info("Encrypted Message:\t '" + encoder.encodeToString(encryptedData) + "'");
            logger.info("Decrypted Message:\t '" + new String(decryptedData).trim() + "'");

            return decryptedData;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public static KVMessage receiveEncryptedMessage(InputStream input, PrivateKey privateKey) throws IOException {
        // read the length of the encrypted key
        byte[] lengthBytes = new byte[4];
        input.read(lengthBytes);
        ByteBuffer lengthByteBuffer = ByteBuffer.wrap(lengthBytes);
        int encryptedKeyLength = lengthByteBuffer.getInt();

        // read the encrypted key
        byte[] encryptedKey = new byte[encryptedKeyLength];
        input.read(encryptedKey);
        ByteBuffer keyByteBuffer = ByteBuffer.wrap(encryptedKey);

        // read the encrypted message
        byte[] encryptedMessage = new byte[input.available()];
        input.read(encryptedMessage);
        ByteBuffer messageByteBuffer = ByteBuffer.wrap(encryptedMessage);

        // combine the encrypted key and message byte buffers
        ByteBuffer combinedByteBuffer = ByteBuffer.allocate(encryptedKeyLength + encryptedMessage.length + Integer.BYTES);
        combinedByteBuffer.putInt(encryptedKey.length);
        combinedByteBuffer.put(encryptedKey);
        combinedByteBuffer.put(encryptedMessage);
        combinedByteBuffer.flip();
        //logger.info("msgBytes is " + Arrays.toString(combinedByteBuffer.array()));
        KVMessage receivedMsg;
        // Decrypt here, an empty message has length 4
        if (combinedByteBuffer.array().length != 4) {
            byte[] msgBytesDecrypted = decrypt(combinedByteBuffer, privateKey);
            receivedMsg = new KVMessage(msgBytesDecrypted);
        } else {
            receivedMsg = new KVMessage("");
        }

        /* build final String */
        //logger.info("Receive message:\t '" + receivedMsg.getMsg() + "'");

        return receivedMsg; 
    }

    public static PublicKey strToPublicKey (String key) {
        try {
            byte[] byteKey = Base64.getDecoder().decode(key);
            X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(byteKey);
            KeyFactory kf = KeyFactory.getInstance("RSA");

            return kf.generatePublic(X509publicKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}