/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.utils;


import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
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

      SSLConnectionSocketFactory sslConFactory = new SSLConnectionSocketFactory(sslContext);

      final HttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
          .setSSLSocketFactory(sslConFactory)
          .build();

      CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(cm).build();

      HttpComponentsClientHttpRequestFactory requestFactory =
          new HttpComponentsClientHttpRequestFactory(httpClient);

      restTemplate = new RestTemplate(requestFactory);
    } catch (Exception e) {
      log.warn("Cannot create Rest Template due to error: ", e);
    }

    return restTemplate;
  }
}
