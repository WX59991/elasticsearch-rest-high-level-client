package com.example.elasticsearch.https;

import java.security.KeyStore;

public class KeyStoreMaterial {

    /**
     * 密码
     */
    private String password;

    /**
     * keyStore
     */
    private KeyStore keyStore;

    public KeyStoreMaterial(String password, KeyStore keyStore) {
        this.keyStore = keyStore;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(KeyStore keyStore) {
        this.keyStore = keyStore;
    }
}