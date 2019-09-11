package com.example.elasticsearch.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author wangxia
 * @date 2019/9/9 17:34
 * @Description: RestHighLevelClient配置类
 */
@Configuration
@Slf4j
@EnableConfigurationProperties(ElasticsearchProperties.class)
public class ElasticsearchRestClientConfig {

    private static final int ADDRESS_LENGTH = 2;
    private static final int IDENTITY_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";


    private final ElasticsearchProperties properties;

    public ElasticsearchRestClientConfig(ElasticsearchProperties properties) {
        this.properties = properties;
    }

    @Bean(name = "highLevelClient")
    public RestHighLevelClient highLevelClient() {
        RestClientBuilder restClientBuilder = restClientBuilder();
        //TODO 此处可以进行其它操作
        return new RestHighLevelClient(restClientBuilder);
    }


    public RestClientBuilder restClientBuilder() {
        HttpHost[] hosts = Arrays.stream(properties.getClusterName().split(","))
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (!properties.getProperties().isEmpty()
                && !StringUtils.isEmpty(properties.getProperties().get("xpack.security.user"))) {
            String[] identity = properties.getProperties().get("xpack.security.user").split(":");
            if (identity.length == IDENTITY_LENGTH) {
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(identity[0], identity[1]));
            }
        }
        return RestClient.builder(hosts)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
    }

    private HttpHost makeHttpHost(String s) {
        assert !StringUtils.isEmpty(s);
        String[] address = s.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            log.info("elasticsearct {}:{} is join",ip ,port);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            return null;
        }
    }

}
