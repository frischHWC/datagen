package com.datagen.views;

import com.datagen.views.analysis.AnalysisView;
import com.datagen.views.commands.CommandsView;
import com.datagen.views.generation.GenerationView;
import com.datagen.views.models.CredentialsView;
import com.datagen.views.models.ModelsCreationView;
import com.datagen.views.models.ModelsManagementView;
import com.vaadin.flow.component.applayout.AppLayout;
import com.vaadin.flow.component.applayout.DrawerToggle;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.orderedlayout.Scroller;
import com.vaadin.flow.component.sidenav.SideNav;
import com.vaadin.flow.component.sidenav.SideNavItem;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.theme.lumo.LumoUtility;
import org.vaadin.lineawesome.LineAwesomeIcon;

/**
 * The main view is a top-level placeholder for other views.
 */
public class MainLayout extends AppLayout {

    private H1 viewTitle;

    public MainLayout() {
        setPrimarySection(Section.DRAWER);
        addDrawerContent();
        addHeaderContent();
    }

    private void addHeaderContent() {
        DrawerToggle toggle = new DrawerToggle();
        toggle.setAriaLabel("Menu toggle");

        viewTitle = new H1();
        viewTitle.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.Margin.AUTO);

        addToNavbar(true, toggle, viewTitle);
    }

    private void addDrawerContent() {
        Span appName = new Span("Datagen");
        appName.addClassNames(LumoUtility.FontWeight.SEMIBOLD, LumoUtility.FontSize.LARGE);
        Header header = new Header(appName);

        Scroller scroller = new Scroller(createNavigation());

        addToDrawer(header, scroller, createFooter());
    }

    private SideNav createNavigation() {
        SideNav nav = new SideNav();

        var homeNav = new SideNavItem("Home", HomeView.class, LineAwesomeIcon.HOME_SOLID.create());

        var modelsNav = new SideNavItem("Models");
        modelsNav.setPrefixComponent(LineAwesomeIcon.PENCIL_RULER_SOLID.create());
        modelsNav.addItem(new SideNavItem("Create", ModelsCreationView.class, LineAwesomeIcon.PEN_SOLID.create()));
        modelsNav.addItem(new SideNavItem("Manage", ModelsManagementView.class, LineAwesomeIcon.PAPERCLIP_SOLID.create()));
        modelsNav.addItem(new SideNavItem("Credentials", CredentialsView.class, LineAwesomeIcon.USER_SECRET_SOLID.create()));

        var dataGenNav = new SideNavItem("Data Generation");
        dataGenNav.setPrefixComponent(LineAwesomeIcon.INFINITY_SOLID.create());
        dataGenNav.addItem(new SideNavItem("Generate", GenerationView.class, LineAwesomeIcon.FIGHTER_JET_SOLID.create()));
        dataGenNav.addItem(new SideNavItem("Commands", CommandsView.class, LineAwesomeIcon.ROCKET_SOLID.create()));

        var dataAnalysisNav = new SideNavItem("Data Analysis");
        dataAnalysisNav.setPrefixComponent(LineAwesomeIcon.COMPASS.create());
        dataAnalysisNav.addItem(new SideNavItem("Analyze", AnalysisView.class, LineAwesomeIcon.PLAY_CIRCLE.create()));

        nav.addItem(homeNav, modelsNav, dataGenNav, dataAnalysisNav);

        return nav;
    }

    private Footer createFooter() {
        Footer footer = new Footer();
        return footer;
    }

    @Override
    protected void afterNavigation() {
        super.afterNavigation();
        viewTitle.setText(getCurrentPageTitle());
    }

    private String getCurrentPageTitle() {
        PageTitle title = getContent().getClass().getAnnotation(PageTitle.class);
        return title == null ? "" : title.value();
    }
}
