package com.example.elasticsearch.https;

import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

public class KeyStoreUtil {

    private static final Object lock = new Object();
    private static Map<String, KeyStoreMaterial> keystores = new HashMap<String, KeyStoreMaterial>();

    /**
     * getKeyStore:获取证书<br/>
     *
     * @param path
     * @param password
     * @return
     * @since JDK 1.7
     */
    public static KeyStoreMaterial getKeyStore(String path, String password) {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(password)) {
            return null;
        }
        String key = formatKey(new String[]{path, password});
        KeyStoreMaterial result = keystores.get(key);
        if (result == null) {
            synchronized (lock) {
                result = keystores.get(key);
                if (result == null) {
                    try {
                        KeyStore keyStore = loadItemTokeyStore(path, password);
                        result = new KeyStoreMaterial(password, keyStore);
                        keystores.put(key, result);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return result;
    }

    /**
     * formatKey:生成key. <br/>
     *
     * @param strs
     * @return
     * @since JDK 1.7
     */
    private static String formatKey(String[] strs) {
        StringBuffer sb = new StringBuffer();
        for (String str : strs) {
            sb.append(str).append("#");
        }
        return sb.toString();
    }

    /**
     * loadItemTokeyStore:获取证书 <br/>
     *
     * @param path
     * @param password
     * @return
     * @throws Exception
     * @since JDK 1.7
     */
    private static KeyStore loadItemTokeyStore(String path, String password) throws Exception {
        KeyStore keyStore = null;
        InputStream is = null;
        if (path.startsWith("classpath:")) {
            path = path.substring("classpath:".length());
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        } else if (path.startsWith("file:")) {
            path = path.substring("file:".length());
            is = new BufferedInputStream(new FileInputStream(path));
        }
        if (is != null) {
            try {
                keyStore = KeyStore.getInstance("PKCS12");
                keyStore.load(is, password.toCharArray());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                is.close();
            }
        }

        return keyStore;
    }

}