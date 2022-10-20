package com.cloudera.frisch.datagen.utils;


import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.TrustStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.security.cert.X509Certificate;

@Slf4j
@Component
public class RestTemplateProvider {

    @Bean
    public RestTemplate provideRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();

        try {

            TrustStrategy acceptingTrustStrategy =
                (X509Certificate[] chain, String authType) -> true;

            SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
                .loadTrustMaterial(null, acceptingTrustStrategy)
                .build();

            SSLConnectionSocketFactory csf =
                new SSLConnectionSocketFactory(sslContext);

            CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLSocketFactory(csf)
                .build();

            HttpComponentsClientHttpRequestFactory requestFactory =
                new HttpComponentsClientHttpRequestFactory();

            requestFactory.setHttpClient(httpClient);

            restTemplate = new RestTemplate(requestFactory);
        } catch (Exception e) {
            log.warn("Cannot create Rest Template due to error: ", e);
        }

        return restTemplate;
    }
}
