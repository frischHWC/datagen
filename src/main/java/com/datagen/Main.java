/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen;

import com.vaadin.flow.component.page.AppShellConfigurator;
import com.vaadin.flow.server.AppShellSettings;
import com.vaadin.flow.theme.Theme;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@SuppressWarnings("unchecked")
@EnableScheduling
@SpringBootApplication
@Theme(value = "datagen")
public class Main implements AppShellConfigurator {

    @Autowired
    private ApplicationContext appContext;

    public static void main(String [] args) {
        SpringApplication.run(Main.class, args);
    }

    @Slf4j
    @Component
    public static class Shutdowner {

        @Autowired
        private ApplicationContext appContext;

        public void initiateShutdown(int returnCode){
            Shutdowner.log.error("Going to shutdown due to previous error");
            SpringApplication.exit(appContext, () -> returnCode);
        }
    }

    @Override
    public void configurePage(AppShellSettings settings) {
        settings.setPageTitle("Datagen");
        settings.addFavIcon("icon", "favicon.icon", "64x64");
    }


}
