/**
 * ***************************************************************************** Copyright (c)
 * 2012-2017 Codenvy, S.A. All rights reserved. This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 * <p>Contributors: Codenvy, S.A. - initial API and implementation
 * *****************************************************************************
 */
package org.eclipse.che.plugin.github.factory.resolver;

import javax.inject.Inject;
import org.eclipse.che.plugin.urlfactory.URLChecker;

/**
 * Support old dockerfila and factory filenames;
 *
 * @author Max Shaposhnik
 */
public class LegacyGithubURLParser extends GithubURLParserImpl {

  private URLChecker urlChecker;

  @Inject
  public LegacyGithubURLParser(URLChecker urlChecker) {
    this.urlChecker = urlChecker;
  }

  @Override
  public GithubUrl parse(String url) {
    GithubUrl githubUrl = super.parse(url);
    if (!urlChecker.exists(githubUrl.dockerFileLocation())) {
      githubUrl.withDockerfileFilename(".codenvy.dockerfile");
    }

    if (!urlChecker.exists(githubUrl.factoryJsonFileLocation())) {
      githubUrl.withFactoryFilename(".codenvy.json");
    }
    return githubUrl;
  }
}
