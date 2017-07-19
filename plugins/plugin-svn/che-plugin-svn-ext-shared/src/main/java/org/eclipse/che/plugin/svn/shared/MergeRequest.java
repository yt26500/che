/**
 * ***************************************************************************** Copyright (c)
 * 2012-2017 Codenvy, S.A. All rights reserved. This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 * <p>Contributors: Codenvy, S.A. - initial API and implementation
 * *****************************************************************************
 */
package org.eclipse.che.plugin.svn.shared;

import javax.validation.constraints.NotNull;
import org.eclipse.che.dto.shared.DTO;

@DTO
public interface MergeRequest {

  /**
   * ************************************************************************
   *
   * <p>Project path
   *
   * <p>************************************************************************
   */
  String getProjectPath();

  void setProjectPath(@NotNull final String projectPath);

  MergeRequest withProjectPath(@NotNull final String projectPath);

  /**
   * ************************************************************************
   *
   * <p>Target
   *
   * <p>************************************************************************
   */
  String getTarget();

  void setTarget(@NotNull final String target);

  MergeRequest withTarget(@NotNull final String target);

  /**
   * ************************************************************************
   *
   * <p>Source
   *
   * <p>************************************************************************
   */
  String getSourceURL();

  void setSourceURL(@NotNull final String sourceURL);

  MergeRequest withSourceURL(@NotNull final String sourceURL);
}
