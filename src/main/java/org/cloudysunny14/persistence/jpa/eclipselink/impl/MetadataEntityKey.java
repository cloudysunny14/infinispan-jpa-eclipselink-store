package org.cloudysunny14.persistence.jpa.eclipselink.impl;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.persistence.Embeddable;

import org.cloudysunny14.persistence.jpa.eclipselink.JpaEclipselinkStoreException;
import org.infinispan.commons.util.Base64;

@Embeddable
public class MetadataEntityKey implements Serializable {

    private static final long serialVersionUID = -760572560532399400L;
    private static final String DIGEST_ALG = "SHA-256";

    private String keySha;

    public MetadataEntityKey() {
    }

    public MetadataEntityKey(byte[] keyBytes) {
        keySha = getKeyBytesSha(keyBytes);
    }

    public String getKeySha() {
        return keySha;
    }

    public void setKeySha(String keySha) {
        this.keySha = keySha;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof MetadataEntityKey))
            return false;
        return keySha.equals(((MetadataEntityKey) o).getKeySha());
    }

    @Override
    public int hashCode() {
        return keySha.hashCode();
    }

    public static String getKeyBytesSha(byte[] keyBytes) {
        String keyBytesSha;
        try {
            MessageDigest digest = MessageDigest.getInstance(DIGEST_ALG);
            byte[] sha = digest.digest(keyBytes);
            keyBytesSha = Base64.encodeBytes(sha);
        } catch(NoSuchAlgorithmException e) {
            throw new JpaEclipselinkStoreException("Failed to create SHA hash of metadata key", e);
        }
        return keyBytesSha;
    }

    @Override
    public String toString() {
        return "MetadataEntityKey{keySha='" + keySha + "'}";
    }
}