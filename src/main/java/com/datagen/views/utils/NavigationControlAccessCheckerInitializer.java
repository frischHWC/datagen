package com.datagen.views.utils;

import com.datagen.views.login.LoginView;
import com.vaadin.flow.server.ServiceInitEvent;
import com.vaadin.flow.server.VaadinServiceInitListener;
import com.vaadin.flow.server.auth.NavigationAccessControl;

public class NavigationControlAccessCheckerInitializer implements VaadinServiceInitListener {

  private NavigationAccessControl accessControl;

  public NavigationControlAccessCheckerInitializer() {
    accessControl = new NavigationAccessControl();
    accessControl.setLoginView(LoginView.class);
  }

  @Override
  public void serviceInit(ServiceInitEvent serviceInitEvent) {
    serviceInitEvent.getSource().addUIInitListener(uiInitEvent -> {
      uiInitEvent.getUI().addBeforeEnterListener(accessControl);
    });
  }
}