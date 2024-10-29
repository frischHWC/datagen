package com.datagen.views;

import com.datagen.views.analysis.AnalysisView;
import com.datagen.views.commands.CommandsView;
import com.datagen.views.generation.GenerationView;
import com.datagen.views.models.CredentialsView;
import com.datagen.views.models.ModelsCreationView;
import com.datagen.views.models.ModelsManagementView;
import com.datagen.views.users.UsersView;
import com.datagen.views.utils.UsersUtils;
import com.vaadin.flow.component.UI;
import com.vaadin.flow.component.applayout.AppLayout;
import com.vaadin.flow.component.applayout.DrawerToggle;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.Scroller;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.sidenav.SideNav;
import com.vaadin.flow.component.sidenav.SideNavItem;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.spring.security.AuthenticationContext;
import com.vaadin.flow.theme.lumo.LumoUtility;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.userdetails.UserDetails;
import org.vaadin.lineawesome.LineAwesomeIcon;

/**
 * The main view is a top-level placeholder for other views.
 */
@Slf4j
public class MainLayout extends AppLayout {

    private H1 viewTitle;
    private final transient AuthenticationContext authContext;

    public MainLayout(AuthenticationContext authContext) {
        this.authContext = authContext;
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

        var usersNav = new SideNavItem("Users");
        usersNav.setPrefixComponent(LineAwesomeIcon.USER.create());
        usersNav.addItem(new SideNavItem("Manage", UsersView.class,
            LineAwesomeIcon.USER_SLASH_SOLID.create()));

        nav.addItem(homeNav, modelsNav, dataGenNav, dataAnalysisNav, usersNav);


        return nav;
    }

    private Footer createFooter() {
        Footer footer = new Footer();
        Anchor anchorGit = new Anchor("https://github.com/frischHWC/datagen", LineAwesomeIcon.GITHUB.create() );
        anchorGit.getElement().setAttribute("target", "_blank");
        Anchor anchorDoc = new Anchor("https://datagener.github.io/", LineAwesomeIcon.BOOK_OPEN_SOLID.create());
        anchorDoc.getElement().setAttribute("target", "_blank");
        //var localUri = Page.fetchCurrentURL();
        var localUri = UI.getCurrent().getActiveViewLocation().getPath();
        Anchor anchorSwagger = new Anchor(localUri + "/swagger-ui.html#/", LineAwesomeIcon.CODE_SOLID.create());
        anchorSwagger.getElement().setAttribute("target", "_blank");
        String version = "1.0.0";

        // User Info
        var userInfo = authContext.getAuthenticatedUser(UserDetails.class)
            .map(user -> {
                return new Span(user.getUsername() + (UsersUtils.isUserDatagenAdmin(authContext)?" (admin) ":" (user) "));
            }).orElseGet(() -> new Span("ANONYMOUS"));
        Button logout = new Button(" Logout ", click -> this.authContext.logout());
        logout.setIcon(LineAwesomeIcon.SIGN_OUT_ALT_SOLID.create());
        logout.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        logout.addThemeVariants(ButtonVariant.LUMO_ERROR);

        var vl = new VerticalLayout();
        vl.add(new HorizontalLayout(LineAwesomeIcon.USER_CIRCLE.create(), userInfo));
        vl.add(logout);
        vl.add(new HorizontalLayout(anchorSwagger, anchorDoc, anchorGit, new Span("V." + version)));

        footer.add(vl);
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
