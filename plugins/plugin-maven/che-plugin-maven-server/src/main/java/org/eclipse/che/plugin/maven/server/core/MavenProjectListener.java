/**
 * ***************************************************************************** Copyright (c)
 * 2012-2017 Codenvy, S.A. All rights reserved. This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 * <p>Contributors: Codenvy, S.A. - initial API and implementation
 * *****************************************************************************
 */
package org.eclipse.che.plugin.maven.server.core;

import java.util.List;
import java.util.Map;
import org.eclipse.che.plugin.maven.server.core.project.MavenProject;
import org.eclipse.che.plugin.maven.server.core.project.MavenProjectModifications;

/** @author Evgen Vidolob */
public interface MavenProjectListener {

  void projectResolved(MavenProject project, MavenProjectModifications modifications);

  void projectUpdated(
      Map<MavenProject, MavenProjectModifications> updated, List<MavenProject> removed);
}
