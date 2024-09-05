package com.datagen.views.utils;

import com.vaadin.flow.component.html.Anchor;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.FlexLayout;
import org.vaadin.lineawesome.LineAwesomeIcon;

public class FooterLayout {

  public static FlexLayout createFooterContent() {
    Anchor anchor = new Anchor("https://github.com/frischHWC/datagen", "Datagen Github" );
    anchor.getElement().setAttribute("target", "_blank");

    // Wrapper to use up extra space
    FlexLayout footerWrapper = new FlexLayout();
    // Actual footer content
    Div footer = new Div();
    footer.add(new Span("Licensed with Apache 2.0. Code available on "), anchor, LineAwesomeIcon.GITHUB.create());
    //footer.getStyle().setBackgroundColor("grey");
    // Align the footer
    footerWrapper.setWidthFull();
    footerWrapper.setAlignItems(FlexComponent.Alignment.END);
    footerWrapper.setAlignContent(FlexLayout.ContentAlignment.CENTER);
    // Make the footer always last in the parent using FlexBox order
    footerWrapper.getElement().getStyle().set("order", "999");
    footerWrapper.add(footer);
    return footerWrapper;
  }
}
