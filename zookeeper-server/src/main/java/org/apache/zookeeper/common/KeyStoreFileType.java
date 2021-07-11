package org.apache.zookeeper.common;


public enum KeyStoreFileType {
    JKS(".jks"), PEM(".pem"), PKCS12(".p12");

    private final String defaultFileExtension;

    KeyStoreFileType(String defaultFileExtension) {
        this.defaultFileExtension = defaultFileExtension;
    }

    
    public String getPropertyValue() {
        return this.name();
    }

    
    public String getDefaultFileExtension() {
        return defaultFileExtension;
    }

    
    public static KeyStoreFileType fromPropertyValue(String propertyValue) {
        if (propertyValue == null || propertyValue.length() == 0) {
            return null;
        }
        return KeyStoreFileType.valueOf(propertyValue.toUpperCase());
    }

    
    public static KeyStoreFileType fromFilename(String filename) {
        int i = filename.lastIndexOf('.');
        if (i >= 0) {
            String extension = filename.substring(i);
            for (KeyStoreFileType storeFileType : KeyStoreFileType.values()) {
                if (storeFileType.getDefaultFileExtension().equals(extension)) {
                    return storeFileType;
                }
            }
        }
        throw new IllegalArgumentException(
                "Unable to auto-detect store file type from file name: " + filename);
    }

    
    public static KeyStoreFileType fromPropertyValueOrFileName(String propertyValue,
                                                               String filename) {
        KeyStoreFileType result = KeyStoreFileType.fromPropertyValue(propertyValue);
        if (result == null) {
            result = KeyStoreFileType.fromFilename(filename);
        }
        return result;
    }
}
