package com.datagen.views.users;

import com.datagen.service.users.UsersService;
import com.datagen.views.MainLayout;
import com.datagen.views.utils.UsersUtils;
import com.vaadin.flow.component.Composite;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.confirmdialog.ConfirmDialog;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.grid.GridSortOrder;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.notification.NotificationVariant;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.PasswordField;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.data.provider.SortDirection;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.spring.security.AuthenticationContext;
import jakarta.annotation.security.RolesAllowed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.vaadin.lineawesome.LineAwesomeIcon;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@PageTitle("Users")
@Route(value = "users", layout = MainLayout.class)
@RolesAllowed({"ROLE_DATAGEN_USER", "ROLE_DATAGEN_ADMIN"})
public class UsersView extends Composite<VerticalLayout> {

    private final transient AuthenticationContext authContext;

    private UsersService usersService;

    @Autowired
    public UsersView(AuthenticationContext authContext, UsersService usersService) {
        this.usersService = usersService;
        this.authContext = authContext;
        VerticalLayout layoutColumn = new VerticalLayout();
        layoutColumn.setWidth("100%");
        layoutColumn.getStyle().set("flex-grow", "1");

        var grid = new Grid<UsersService.UserDatagen>();
        grid.addColumn(UsersService.UserDatagen::getUsername)
            .setHeader("Username")
            .setSortable(true)
            .setWidth("15rem")
            .setFlexGrow(0);

        // Button to print model and download it
        grid.addColumn(infoButton())
            .setHeader("Info")
            .setWidth("5rem")
            .setFlexGrow(0);

        if(this.usersService.isUserFromInternal()) {
            // Delete button
            grid.addColumn(deleteButton(grid))
                .setHeader("Delete")
                .setWidth("5rem")
                .setFlexGrow(0);

            // RightsButton
            grid.addColumn(rightsButton())
                .setHeader("Manage")
                .setWidth("5rem")
                .setFlexGrow(0);

            grid.addColumn(resetPassword())
                .setHeader("Reset Password")
                .setWidth("10rem")
                .setFlexGrow(0);
        }

        grid.addColumn(u -> u.getGroups().stream().collect(Collectors.joining(" , ")))
            .setHeader("Groups")
            .setSortable(true)
            .setAutoWidth(true);

        grid.setItems(this.usersService.listUsers());
        grid.sort(List.of(new GridSortOrder<>(grid.getColumns().get(0), SortDirection.ASCENDING)));

        var refreshButton = refreshGridButton(grid);
        var hlrefresh = new HorizontalLayout();
        hlrefresh.add(refreshButton);
        if(UsersUtils.isUserDatagenAdmin(authContext)) {
            var addUserButton = addUserButton(grid);
            hlrefresh.add(addUserButton);
        }
        layoutColumn.add(hlrefresh);
        layoutColumn.add(grid);
        getContent().add(layoutColumn);

    }


    private Button addUserButton(Grid<UsersService.UserDatagen> grid) {
        var addButton = new Button("Add", LineAwesomeIcon.USER_CIRCLE.create());
        addButton.setIconAfterText(true);

        addButton.addClickListener(e -> {
            var dialog = new Dialog();
            dialog.setHeaderTitle("Add user: ");

            var vlOfDialog = new VerticalLayout();
            var username = new TextField("Username");
            username.setRequired(true);
            username.setClearButtonVisible(true);
            var password = new PasswordField("Password");
            password.setRequired(true);
            password.setRevealButtonVisible(true);
            var groups = new TextField("Groups");
            groups.setHelperText("A ',' separated list of groups to assign to this user");

            vlOfDialog.add(username, password, groups);
            dialog.add(vlOfDialog);

            var save = new Button("Save", esave -> {
               var success = this.usersService.addUser(username.getValue(),
                   Arrays.stream(groups.getValue().split(",")).collect(
                       Collectors.toSet()), password.getValue());
               if(success) {
                   Notification.show("Successfully added user: " + username.getValue())
                       .addThemeVariants(NotificationVariant.LUMO_SUCCESS);
                   grid.setItems(this.usersService.listUsers());
                   dialog.close();
               } else {
                   Notification.show("Unable to add user: " + username.getValue() + " . Check logs for more details.")
                       .addThemeVariants(NotificationVariant.LUMO_ERROR);
               }
            });
            save.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
            dialog.getFooter().add(save);

            var close = new Button("Close", eclose -> dialog.close());
            dialog.getFooter().add(close);

            dialog.open();
        });

        return addButton;
    }


    private Button refreshGridButton(Grid<UsersService.UserDatagen> grid) {
        var refreshButton = new Button("Refresh", VaadinIcon.REFRESH.create());
        refreshButton.setIconAfterText(true);
        refreshButton.addClickListener( e -> {
            grid.setItems(this.usersService.listUsers());
        });
        return refreshButton;
    }

    private ComponentRenderer<Button, UsersService.UserDatagen> infoButton() {
        return new ComponentRenderer<>(Button::new, (button, userDatagen) -> {
            button.addThemeVariants(ButtonVariant.LUMO_ICON,
                ButtonVariant.LUMO_TERTIARY);
            button.addClickListener(e -> {
                Dialog dialogInfo = new Dialog();
                dialogInfo.setHeaderTitle("User: " + userDatagen.getUsername());
                dialogInfo.add(new Span("Groups: " + String.join(",", userDatagen.getGroups())));

                var closeButton = new Button("Close", eventClose -> dialogInfo.close());

                dialogInfo.getFooter().add(closeButton);
                dialogInfo.open();
            });
            button.setIcon(LineAwesomeIcon.INFO_CIRCLE_SOLID.create());
        });
    }


    private ComponentRenderer<Button, UsersService.UserDatagen> deleteButton(Grid grid) {
        // Dialog
        ConfirmDialog dialog = new ConfirmDialog();
        dialog.setText("Are you sure you want to permanently delete this user?");
        dialog.setCancelable(true);
        dialog.setConfirmText("Delete");
        dialog.setConfirmButtonTheme("error primary");

        return new ComponentRenderer<>(Button::new, (button, userDatagen) -> {
            button.addThemeVariants(ButtonVariant.LUMO_ICON,
                ButtonVariant.LUMO_ERROR);
            button.addClickListener(e -> {
                dialog.setHeader("Delete User : " + userDatagen.getUsername());
                dialog.addConfirmListener(event -> {
                    usersService.deleteUser(userDatagen.getUsername());
                    grid.getDataProvider().refreshAll();
                    Notification notification = Notification.show("Deleted User:" + userDatagen.getUsername());
                    notification.addThemeVariants(NotificationVariant.LUMO_PRIMARY);
                });
                dialog.open();
            });

            if(UsersUtils.isUserDatagenAdmin(authContext)) {
                button.setTooltipText("Your user is not admin and has no right to delete this user. Check User Permission.");
                button.setIcon(LineAwesomeIcon.TRASH_ALT.create());
                button.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
            } else {
                button.setEnabled(false);
                button.setIcon(LineAwesomeIcon.TRASH_ALT_SOLID.create());
                button.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
            }
        });
    }


    private ComponentRenderer<Button, UsersService.UserDatagen> rightsButton() {
        return new ComponentRenderer<>(Button::new, (button, userDatagen) -> {
            button.addThemeVariants(ButtonVariant.LUMO_ICON,
                ButtonVariant.LUMO_TERTIARY);

            button.addClickListener(e -> {
                Dialog dialogInfo = new Dialog();
                dialogInfo.setHeaderTitle("User: " + userDatagen.getUsername());

                var metaVL = new VerticalLayout();

                var groups = new TextField("Groups:");
                groups.setClearButtonVisible(true);
                groups.setHelperText("A ',' separated list of groups for this user");
                groups.setValue(String.join(",", userDatagen.getGroups()));

                metaVL.add(groups);
                dialogInfo.add(metaVL);

                // Only admin users can add or delete groups
                if(UsersUtils.isUserDatagenAdmin(authContext)) {
                    var saveButton = new Button("Save", event -> {
                        var success = this.usersService.setGroupsToUser(userDatagen.getUsername(),
                            Arrays.stream(groups.getValue().split(",")).collect(Collectors.toSet()));
                        if(success) {
                            Notification.show(
                                    "Successfully changed groups for user: " + userDatagen.getUsername())
                                .addThemeVariants(NotificationVariant.LUMO_SUCCESS);
                            dialogInfo.close();
                        } else {
                            Notification.show(
                                    "Unable to change groups for user: " + userDatagen.getUsername() + " check server logs for more details")
                                .addThemeVariants(NotificationVariant.LUMO_ERROR);
                        }
                    });
                    saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

                    dialogInfo.getFooter().add(saveButton);
                } else {
                    groups.setEnabled(false);
                    metaVL.add(new Span("You are not an admin, so cannot modify its rights."));
                }

                var closeButton = new Button("Close", eventClose -> dialogInfo.close());

                dialogInfo.getFooter().add(closeButton);
                dialogInfo.open();
            });
            button.setIcon(LineAwesomeIcon.ACCESSIBLE_ICON.create());
        });
    }


    private ComponentRenderer<Button, UsersService.UserDatagen> resetPassword() {
        return new ComponentRenderer<>(Button::new, (button, userDatagen) -> {
            button.addThemeVariants(ButtonVariant.LUMO_ICON,
                ButtonVariant.LUMO_ERROR,
                ButtonVariant.LUMO_TERTIARY);

            if(UsersUtils.isUserDatagenAdmin(authContext) || userDatagen.getUsername().equalsIgnoreCase(UsersUtils.getUser(authContext))) {

                button.addClickListener(e -> {
                    Dialog dialogInfo = new Dialog();
                    dialogInfo.setHeaderTitle(
                        "New Password for user: " + userDatagen.getUsername());

                    var metaVL = new VerticalLayout();

                    var passwordField = new PasswordField("New Password:");
                    passwordField.setClearButtonVisible(true);
                    passwordField.setTooltipText("New Password to set for this user");
                    passwordField.setValue(String.join(",", userDatagen.getGroups()));

                    metaVL.add(passwordField);
                    dialogInfo.add(metaVL);

                    var saveButton = new Button("Update", event -> {
                        var success = this.usersService.updatePasswordOfUser(
                            userDatagen.getUsername(), passwordField.getValue());
                        if (success) {
                            Notification.show(
                                    "Successfully changed password for user: " +
                                        userDatagen.getUsername())
                                .addThemeVariants(
                                    NotificationVariant.LUMO_SUCCESS);
                            dialogInfo.close();
                        } else {
                            Notification.show(
                                    "Unable to change password for user: " +
                                        userDatagen.getUsername() +
                                        " check server logs for more details")
                                .addThemeVariants(
                                    NotificationVariant.LUMO_ERROR);
                        }
                    });
                    saveButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);

                    dialogInfo.getFooter().add(saveButton);

                    var closeButton =
                        new Button("Close", eventClose -> dialogInfo.close());

                    dialogInfo.getFooter().add(closeButton);
                    dialogInfo.open();
                });
            } else {
                button.setEnabled(false);
            }

            button.setIcon(VaadinIcon.RECYCLE.create());
        });
    }




}
