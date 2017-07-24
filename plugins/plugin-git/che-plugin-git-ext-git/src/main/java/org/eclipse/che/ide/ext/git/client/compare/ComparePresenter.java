/*******************************************************************************
 * Copyright (c) 2012-2017 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 *******************************************************************************/
package org.eclipse.che.ide.ext.git.client.compare;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.web.bindery.event.shared.EventBus;

import org.eclipse.che.commons.annotation.Nullable;
import org.eclipse.che.ide.api.event.FileContentUpdateEvent;
import org.eclipse.che.ide.api.git.GitServiceClient;
import org.eclipse.che.ide.api.app.AppContext;
import org.eclipse.che.ide.api.notification.NotificationManager;
import org.eclipse.che.ide.api.resources.Container;
import org.eclipse.che.ide.api.resources.File;
import org.eclipse.che.ide.ext.git.client.GitLocalizationConstant;
import org.eclipse.che.ide.ext.git.client.compare.FileStatus.Status;
import org.eclipse.che.ide.api.dialogs.CancelCallback;
import org.eclipse.che.ide.api.dialogs.ConfirmCallback;
import org.eclipse.che.ide.api.dialogs.DialogFactory;
import org.eclipse.che.ide.resource.Path;

import static org.eclipse.che.ide.api.notification.StatusNotification.DisplayMode.NOT_EMERGE_MODE;
import static org.eclipse.che.ide.api.notification.StatusNotification.Status.FAIL;
import static org.eclipse.che.ide.ext.git.client.compare.FileStatus.Status.ADDED;
import static org.eclipse.che.ide.ext.git.client.compare.FileStatus.Status.DELETED;

/**
 * Presenter for comparing current files with files from specified revision or branch.
 *
 * @author Igor Vinokur
 * @author Vlad Zhukovskyi
 */
@Singleton
public class ComparePresenter implements CompareView.ActionDelegate {

    private final AppContext              appContext;
    private final EventBus                eventBus;
    private final DialogFactory           dialogFactory;
    private final CompareView             view;
    private final GitServiceClient        service;
    private final GitLocalizationConstant locale;
    private final NotificationManager     notificationManager;

    private File    comparedFile;
    private String  revision;
    private String  localContent;
    private boolean compareWithLatest;

    @Inject
    public ComparePresenter(AppContext appContext,
                            EventBus eventBus,
                            DialogFactory dialogFactory,
                            CompareView view,
                            GitServiceClient service,
                            GitLocalizationConstant locale,
                            NotificationManager notificationManager) {
        this.appContext = appContext;
        this.eventBus = eventBus;
        this.dialogFactory = dialogFactory;
        this.view = view;
        this.service = service;
        this.locale = locale;
        this.notificationManager = notificationManager;
        this.view.setDelegate(this);
    }

    /**
     * Show compare window.
     *
     * @param file
     *         file name with its full path
     * @param status
     *         status of the file
     * @param revision
     *         hash of revision or branch
     */
    public void showCompareWithLatest(final File file, final Status status, final String revision) {
        this.comparedFile = file;
        this.revision = revision;
        this.compareWithLatest = true;

        if (status.equals(ADDED)) {
            showCompare("");
            return;
        }

        final Container rootProject = file.getRootProject();

        if (rootProject == null) {
            return;
        }

        final Path relPath = file.getLocation().removeFirstSegments(rootProject.getLocation().segmentCount());

        if (status.equals(DELETED)) {
            service.showFileContent(rootProject.getLocation(), relPath, revision)
                   .then(content -> {
                       view.setTitle(file.getLocation().toString());
                       view.setColumnTitles(locale.compareYourVersionTitle(), revision + locale.compareReadOnlyTitle());
                       view.show(content.getContent(), "", file.getLocation().toString(), false);
                   })
                   .catchError(error -> {
                       notificationManager.notify(error.getMessage(), FAIL, NOT_EMERGE_MODE);
                   });
        } else {
            service.showFileContent(rootProject.getLocation(), relPath, revision)
                   .then(content -> {
                       showCompare(content.getContent());
                   })
                   .catchError(error -> {
                       notificationManager.notify(error.getMessage(), FAIL, NOT_EMERGE_MODE);
                   });
        }
    }

    /**
     * @param file
     *         path of the file
     * @param status
     *         status of the file
     * @param revisionA
     *         hash of the first revision or branch.
     *         If it is set to {@code null}, compare with empty repository state will be performed
     * @param revisionB
     *         hash of the second revision or branch.
     *         If it is set to {@code null}, compare with latest repository state will be performed
     */
    public void showCompareBetweenRevisions(final Path file,
                                            final Status status,
                                            @Nullable final String revisionA,
                                            @Nullable final String revisionB) {
        this.compareWithLatest = false;

        final Path projectLocation = appContext.getRootProject().getLocation();

        view.setTitle(file.toString());
        if (status == Status.ADDED) {
            service.showFileContent(projectLocation, file, revisionB)
                   .then(response -> {
                       view.setColumnTitles(revisionB + locale.compareReadOnlyTitle(),
                                            revisionA == null ? "" : revisionA + locale.compareReadOnlyTitle());
                       view.show("", response.getContent(), file.toString(), true);
                   })
                   .catchError(error -> {
                       notificationManager.notify(error.getMessage(), FAIL, NOT_EMERGE_MODE);
                   });
        } else if (status == Status.DELETED) {
            service.showFileContent(projectLocation, file, revisionA)
                   .then(response -> {
                       view.setColumnTitles(revisionB + locale.compareReadOnlyTitle(), revisionA + locale.compareReadOnlyTitle());
                       view.show(response.getContent(), "", file.toString(), true);
                   })
                   .catchError(error -> {
                       notificationManager.notify(error.getMessage(), FAIL, NOT_EMERGE_MODE);
                   });
        } else {
            service.showFileContent(projectLocation, file, revisionA)
                   .then(contentAResponse -> {
                       service.showFileContent(projectLocation, file, revisionB)
                              .then(contentBResponse -> {
                                  view.setColumnTitles(revisionB + locale.compareReadOnlyTitle(),
                                                       revisionA + locale.compareReadOnlyTitle());
                                  view.show(contentAResponse.getContent(), contentBResponse.getContent(), file.toString(), true);
                              })
                              .catchError(error -> {
                                  notificationManager.notify(error.getMessage(), FAIL, NOT_EMERGE_MODE);
                              });
                   });
        }
    }

    @Override
    public void onClose(final String newContent) {
        if (!compareWithLatest || this.localContent == null || newContent.equals(localContent)) {
            view.hide();
            return;
        }

        ConfirmCallback confirmCallback = () -> comparedFile.updateContent(newContent)
                                                            .then(ignored -> {
                                                                final Container parent = comparedFile.getParent();

                                                                if (parent != null) {
                                                                    parent.synchronize();
                                                                }

                                                                eventBus.fireEvent(new FileContentUpdateEvent(comparedFile.getLocation()
                                                                                                                          .toString()));
                                                                view.hide();
                                                            });

        CancelCallback cancelCallback = view::hide;

        dialogFactory.createConfirmDialog(locale.compareSaveTitle(), locale.compareSaveQuestion(), locale.buttonYes(), locale.buttonNo(),
                                          confirmCallback, cancelCallback).show();
    }

    private void showCompare(final String remoteContent) {
        comparedFile.getContent().then(local -> {
            localContent = local;
            final String path = comparedFile.getLocation().removeFirstSegments(1).toString();
            view.setTitle(path);
            view.setColumnTitles(locale.compareYourVersionTitle(), revision + locale.compareReadOnlyTitle());
            view.show(remoteContent, localContent, path, false);
        });
    }
}
