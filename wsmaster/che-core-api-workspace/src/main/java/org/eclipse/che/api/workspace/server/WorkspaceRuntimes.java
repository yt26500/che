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
package org.eclipse.che.api.workspace.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.eclipse.che.api.core.ConflictException;
import org.eclipse.che.api.core.NotFoundException;
import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.core.ValidationException;
import org.eclipse.che.api.core.model.workspace.Workspace;
import org.eclipse.che.api.core.model.workspace.WorkspaceStatus;
import org.eclipse.che.api.core.model.workspace.config.Environment;
import org.eclipse.che.api.core.model.workspace.runtime.RuntimeIdentity;
import org.eclipse.che.api.core.notification.EventService;
import org.eclipse.che.api.core.notification.EventSubscriber;
import org.eclipse.che.api.workspace.server.model.impl.EnvironmentImpl;
import org.eclipse.che.api.workspace.server.model.impl.RuntimeImpl;
import org.eclipse.che.api.workspace.server.model.impl.WorkspaceImpl;
import org.eclipse.che.api.workspace.server.spi.InfrastructureException;
import org.eclipse.che.api.workspace.server.spi.InternalRuntime;
import org.eclipse.che.api.workspace.server.spi.RuntimeContext;
import org.eclipse.che.api.workspace.server.spi.RuntimeIdentityImpl;
import org.eclipse.che.api.workspace.server.spi.RuntimeInfrastructure;
import org.eclipse.che.api.workspace.shared.dto.event.RuntimeStatusEvent;
import org.eclipse.che.api.workspace.shared.dto.event.WorkspaceStatusEvent;
import org.eclipse.che.commons.env.EnvironmentContext;
import org.eclipse.che.commons.lang.concurrent.StripedLocks;
import org.eclipse.che.commons.lang.concurrent.ThreadLocalPropagateContext;
import org.eclipse.che.commons.lang.concurrent.Unlocker;
import org.eclipse.che.commons.subject.Subject;
import org.eclipse.che.dto.server.DtoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.RUNNING;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.STARTING;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.STOPPED;
import static org.eclipse.che.api.core.model.workspace.WorkspaceStatus.STOPPING;

// TODO: spi: deal with exceptions

/**
 * Defines an internal API for managing {@link RuntimeImpl} instances.
 *
 * @author Yevhenii Voevodin
 * @author Alexander Garagatyi
 */
@Singleton
public class WorkspaceRuntimes {

    private static final Logger LOG = LoggerFactory.getLogger(WorkspaceRuntimes.class);

    private final ImmutableMap<String, RuntimeInfrastructure> infraByRecipe;

    private final ConcurrentMap<String, RuntimeState> runtimes;
    private final EventService                        eventService;
    private final WorkspaceSharedPool                 sharedPool;
    private final StripedLocks                        locks;

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicBoolean isStartRefused = new AtomicBoolean(false);

    @Inject
    public WorkspaceRuntimes(EventService eventService,
                             Set<RuntimeInfrastructure> infrastructures,
                             WorkspaceSharedPool sharedPool) {
        this.runtimes = new ConcurrentHashMap<>();
        this.eventService = eventService;
        this.sharedPool = sharedPool;
        // 16 - experimental value for stripes count, it comes from default hash map size
        this.locks = new StripedLocks(16);

        // TODO: consider extracting to a strategy interface(1. pick the last, 2. fail with conflict)
        Map<String, RuntimeInfrastructure> tmp = new HashMap<>();
        for (RuntimeInfrastructure infra : infrastructures) {
            for (String type : infra.getRecipeTypes()) {
                LOG.info("Register infrastructure '{}' recipe type '{}'", infra.getName(), type);
                RuntimeInfrastructure existingInfra = tmp.put(type, infra);
                if (existingInfra != null) {
                    LOG.warn("Both '{}' and '{}' infrastructures support recipe of type '{}', infrastructure '{}' will be used",
                             infra.getName(),
                             existingInfra.getName(),
                             type,
                             infra.getName());
                }
            }
        }
        infraByRecipe = ImmutableMap.copyOf(tmp);
    }

    // TODO Doesn't look like correct place for this logic. Where this code should be?
    public Environment estimate(Environment environment) throws NotFoundException, InfrastructureException, ValidationException {
        // TODO decide whether throw exception when dev machine not found
        String type = environment.getRecipe().getType();
        if (!infraByRecipe.containsKey(type)) {
            throw new NotFoundException("Infrastructure not found for type: " + type);
        }

        return infraByRecipe.get(type).estimate(environment);
    }

    /**
     * Injects runtime information such as status and {@link org.eclipse.che.api.core.model.workspace.Runtime}
     * into the workspace object, if the workspace doesn't have runtime sets the
     * status to {@link WorkspaceStatus#STOPPED}.
     *
     * @param workspace
     *         the workspace to inject runtime into
     */
    public void injectRuntime(WorkspaceImpl workspace) {
        RuntimeState runtimeState = runtimes.get(workspace.getId());
        if (runtimeState != null) {
            workspace.setRuntime(new RuntimeImpl(runtimeState.runtime));
            workspace.setStatus(runtimeState.status);
        } else {
            workspace.setStatus(STOPPED);
        }
    }

    /**
     * Injects workspace status into provided workspace.
     *
     * @param workspace
     *         the workspace to inject runtime into
     */
    public void injectStatus(WorkspaceImpl workspace) {
        RuntimeState state = runtimes.get(workspace.getId());
        if (state != null) {
            workspace.setStatus(state.status);
        } else {
            workspace.setStatus(STOPPED);
        }
    }

    /**
     * Starts all machines from specified workspace environment,
     * creates workspace runtime instance based on that environment.
     * <p>
     * <p>During the start of the workspace its
     * runtime is visible with {@link WorkspaceStatus#STARTING} status.
     *
     * @param workspace
     *         workspace which environment should be started
     * @param envName
     *         the name of the environment to start
     * @param options
     *         whether machines should be recovered(true) or not(false)
     * @return the workspace runtime instance with machines set.
     * @throws ConflictException
     *         when workspace is already running
     * @throws ConflictException
     *         when start is interrupted
     * @throws NotFoundException
     *         when any not found exception occurs during environment start
     * @throws ServerException
     *         other error occurs during environment start
     * @see WorkspaceStatus#STARTING
     * @see WorkspaceStatus#RUNNING
     */
    public CompletableFuture<Void> startAsync(Workspace workspace,
                                              String envName,
                                              Map<String, String> options)
            throws ConflictException, NotFoundException, ServerException {

        final EnvironmentImpl environment = copyEnv(workspace, envName);
        final String workspaceId = workspace.getId();


        requireNonNull(environment, "Environment should not be null " + workspaceId);
        requireNonNull(environment.getRecipe(), "OldRecipe should not be null " + workspaceId);
        requireNonNull(environment.getRecipe().getType(), "OldRecipe type should not be null " + workspaceId);

        RuntimeInfrastructure infra = infraByRecipe.get(environment.getRecipe().getType());
        if (infra == null) {
            throw new NotFoundException("No infrastructure found of type: " + environment.getRecipe().getType() +
                                        " for workspace: " + workspaceId);
        }

        RuntimeState existingState = runtimes.get(workspaceId);
        if (existingState != null) {
            throw new ConflictException(
                    format("Could not start workspace '%s' because its state is '%s'",
                           workspaceId, existingState.status));
        }

        Subject subject = EnvironmentContext.getCurrent().getSubject();
        RuntimeIdentity runtimeId = new RuntimeIdentityImpl(workspaceId, envName, subject.getUserName());
        try (@SuppressWarnings("unused") Unlocker u = locks.writeLock(workspaceId)) {
            checkIsNotTerminated("start the workspace");
            if (isStartRefused.get()) {
                throw new ConflictException(format("Start of the workspace '%s' is rejected by the system, " +
                                                   "no more workspaces are allowed to start",
                                                   workspace.getConfig().getName()));
            }
            RuntimeContext runtimeContext = infra.prepare(runtimeId, environment);

            InternalRuntime runtime = runtimeContext.getRuntime();
            if (runtime == null) {
                throw new IllegalStateException(
                        "SPI contract violated. RuntimeInfrastructure.start(...) must not return null: "
                        + RuntimeInfrastructure.class);
            }
            RuntimeState state = new RuntimeState(runtime, STARTING);
            if (runtimes.putIfAbsent(workspaceId, state) != null) {
                throw new ConflictException("Could not start workspace '" + workspaceId +
                                            "' because it is not in 'STOPPED' state");
            }
            eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                           .withWorkspaceId(workspaceId)
                                           .withStatus(STARTING)
                                           .withPrevStatus(STOPPED));
            return CompletableFuture.runAsync(ThreadLocalPropagateContext.wrap(() -> {
                try {
                    runtime.start(options);
                    runtimes.replace(workspaceId, new RuntimeState(runtime, RUNNING));
                    eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                                   .withWorkspaceId(workspaceId)
                                                   .withStatus(WorkspaceStatus.RUNNING)
                                                   .withPrevStatus(STARTING));
                } catch (InfrastructureException e) {
                    runtimes.remove(workspaceId);
                    eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                                   .withWorkspaceId(workspaceId)
                                                   .withStatus(STOPPED)
                                                   .withPrevStatus(STARTING)
                                                   .withError(e.getMessage()));
                    throw new RuntimeException(e);
                }
            }), sharedPool.getExecutor());
            //TODO made complete rework of exceptions.
        } catch (ValidationException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new ConflictException(e.getLocalizedMessage());
        } catch (InfrastructureException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new ServerException(e.getLocalizedMessage(), e);
        }
    }

    private void checkIsNotTerminated(String operation) throws ServerException {
        if (isShutdown.get()) {
            throw new ServerException("Could not " + operation + " because workspaces service is being terminated");
        }
    }

    /**
     * Stops running workspace runtime.
     * <p>
     * <p>Stops environment in an implementation specific way.
     * During the stop of the workspace its runtime is accessible with {@link WorkspaceStatus#STOPPING stopping} status.
     * Workspace may be stopped only if its status is {@link WorkspaceStatus#RUNNING}.
     *
     * @param workspaceId
     *         identifier of workspace which should be stopped
     * @throws NotFoundException
     *         when workspace with specified identifier does not have runtime
     * @throws ConflictException
     *         when running workspace status is different from {@link WorkspaceStatus#RUNNING}
     * @throws InfrastructureException
     *         when any other error occurs during workspace stopping
     * @see WorkspaceStatus#STOPPING
     */
    public void stop(String workspaceId, Map<String, String> options) throws NotFoundException,
                                                                             InfrastructureException,
                                                                             ConflictException {
        final RuntimeState state = runtimes.get(workspaceId);
        if (state == null) {
            throw new NotFoundException("Workspace with id '" + workspaceId + "' is not running.");
        }
        if (!state.status.equals(RUNNING)) {
            throw new ConflictException(
                    format("Could not stop workspace '%s' because its state is '%s'", workspaceId, state.status));
        }
        if (!runtimes.replace(workspaceId, state, new RuntimeState(state.runtime, STOPPING))) {
            RuntimeState newState = runtimes.get(workspaceId);
            WorkspaceStatus status = newState != null ? newState.status : STOPPED;
            throw new ConflictException(
                    format("Could not stop workspace '%s' because its state is '%s'", workspaceId, status));
        }
        eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                       .withWorkspaceId(workspaceId)
                                       .withPrevStatus(WorkspaceStatus.RUNNING)
                                       .withStatus(WorkspaceStatus.STOPPING));

        try {
            state.runtime.stop(options);

            // remove before firing an event to have consistency between state and the event
            runtimes.remove(workspaceId);
            eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                           .withWorkspaceId(workspaceId)
                                           .withPrevStatus(WorkspaceStatus.STOPPING)
                                           .withStatus(STOPPED));
        } catch (InfrastructureException e) {
            // remove before firing an event to have consistency between state and the event
            runtimes.remove(workspaceId);
            eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                           .withWorkspaceId(workspaceId)
                                           .withPrevStatus(WorkspaceStatus.STOPPING)
                                           .withStatus(STOPPED)
                                           .withError("Error occurs on workspace runtime stop. Error: " +
                                                      e.getMessage()));
        }
    }

    /**
     * Returns true if workspace was started and its status is
     * {@link WorkspaceStatus#RUNNING running}, {@link WorkspaceStatus#STARTING starting}
     * or {@link WorkspaceStatus#STOPPING stopping} - otherwise returns false.
     *
     * @param workspaceId
     *         workspace identifier to perform check
     * @return true if workspace is running, otherwise false
     */
    public boolean hasRuntime(String workspaceId) {
        return runtimes.containsKey(workspaceId);
    }

    @PostConstruct
    private void init() {
        recover();
        subscribeCleanupOnAbnormalRuntimeStopEvent();
    }

    private void recover() {
        for (RuntimeInfrastructure infra : infraByRecipe.values()) {
            try {
                for (RuntimeIdentity id : infra.getIdentities()) {
                    // TODO how to identify correct state of runtime
                    if (runtimes.putIfAbsent(id.getWorkspaceId(),
                                             new RuntimeState(validate(infra.getRuntime(id)), RUNNING)) != null) {
                        // should not happen, violation of SPI contract
                        LOG.error("More than 1 runtime of workspace found. " +
                                  "Runtime identity of duplicate is '{}'. Skipping duplicate.", id);
                    }
                }
            } catch (UnsupportedOperationException x) {
                LOG.warn("Not recoverable infrastructure: '{}'", infra.getName());
            } catch (InfrastructureException x) {
                LOG.error("An error occurred while attempted to recover runtimes using infrastructure '{}'. Reason: '{}'",
                          infra.getName(),
                          x.getMessage());
            }
        }
    }

    private void subscribeCleanupOnAbnormalRuntimeStopEvent() {
        eventService.subscribe(new CleanupRuntimeOnAbnormalRuntimeStop());
    }

    //TODO do we need some validation on start?
    private InternalRuntime validate(InternalRuntime runtime) {
        return runtime;
    }

    private static EnvironmentImpl copyEnv(Workspace workspace, String envName) {

        requireNonNull(workspace, "Workspace should not be null.");
        requireNonNull(envName, "Environment name should not be null.");
        final Environment environment = workspace.getConfig().getEnvironments().get(envName);
        if (environment == null) {
            throw new IllegalArgumentException(format("Workspace '%s' doesn't contain environment '%s'",
                                                      workspace.getId(),
                                                      envName));
        }
        return new EnvironmentImpl(environment);
    }

    public boolean refuseWorkspacesStart() {
        return isStartRefused.compareAndSet(false, true);
    }

    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            throw new IllegalStateException("Workspace runtimes service shutdown has been already called");
        }

        final Set<RuntimeState> states;
        try (@SuppressWarnings("unused") Unlocker u = locks.writeAllLock()) {
            states = ImmutableSet.copyOf(runtimes.values());
            runtimes.clear();
        }

        if (!states.isEmpty()) {
            LOG.info("Shutdown running environments, environments to stop: '{}'", states.size());
            ExecutorService pool =
                    Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
                                                 new ThreadFactoryBuilder().setNameFormat("StopEnvironmentsPool-%d")
                                                                           .setDaemon(false)
                                                                           .build());


            for (RuntimeState state : states) {
                pool.submit(() -> {
                    try {
                        state.runtime.stop(null);
                        // might be already stopped
                    } catch (Exception x) {
                        LOG.error(x.getMessage(), x);
                    }
                });
            }

            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                    if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                        LOG.error("Unable to stop runtimes termination pool");
                    }
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }







    }

    public Set<String> getRuntimesIds() {
        // TODO get runtime ids
        return null;
    }

    public boolean isAnyRunning() {
        // TODO check if any running
        return false;
    }

    public WorkspaceStatus getStatus(String workspaceId) {
        // TODO get status of the workspace
        return null;
    }

    private class CleanupRuntimeOnAbnormalRuntimeStop implements EventSubscriber<RuntimeStatusEvent> {
        @Override
        public void onEvent(RuntimeStatusEvent event) {
            if (event.isFailed()) {
                RuntimeState state = runtimes.remove(event.getIdentity().getWorkspaceId());
                if (state != null) {
                    eventService.publish(DtoFactory.newDto(WorkspaceStatusEvent.class)
                                                   .withWorkspaceId(state.runtime.getContext().getIdentity()
                                                                         .getWorkspaceId())
                                                   .withPrevStatus(WorkspaceStatus.RUNNING)
                                                   .withStatus(STOPPED)
                                                   .withError("Error occurs on workspace runtime stop. Error: " +
                                                              event.getError()));
                }
            }
        }
    }

    private static class RuntimeState {
        final InternalRuntime runtime;
        final WorkspaceStatus status;

        RuntimeState(InternalRuntime runtime, WorkspaceStatus status) {
            this.runtime = runtime;
            this.status = status;
        }
    }


//
//    private void ensurePreDestroyIsNotExecuted() throws ServerException {
//        if (isPreDestroyInvoked) {
//            throw new ServerException("Could not perform operation because application server is stopping");
//        }
//    }
//

//    /**
//     * Safely compares current status of given workspace
//     * with {@code from} and if they are equal sets the status to {@code to}.
//     * Returns true if the status of workspace was updated with {@code to} value.
//     */
//    private boolean compareAndSetStatus(String id, WorkspaceStatus from, WorkspaceStatus to) {
//        try (@SuppressWarnings("unused") CloseableLock l = locks.acquireWriteLock(id)) {
//            WorkspaceState state = workspaces.get(id);
//            if (state != null && state.getStatus() == from) {
//                state.status = to;
//                return true;
//            }
//        }
//        return false;
//    }
//
}
