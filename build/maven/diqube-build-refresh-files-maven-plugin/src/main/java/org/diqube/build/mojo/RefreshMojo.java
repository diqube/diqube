/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.build.mojo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.DirectoryScanner;
import org.sonatype.plexus.build.incremental.BuildContext;

import com.google.common.io.ByteStreams;

/**
 * "Refreshes" files built by another Mojo and optionally copies the refreshed files to some folders.
 * 
 * <p>
 * When using Eclipse with a maven build, eclipse depends on the plugins correctly "refreshing" their output files if
 * their contents changed. If a specific plugin does not do this correctly, Eclipse misses the changes and won't execute
 * any (for example) maven-resource-plugin on those resources any more, as it thinks the resource did not change.
 * 
 * <p>
 * This plugin allows to mark files as refreshed forcibly.
 */
@Mojo(name = "refresh")
public class RefreshMojo extends AbstractMojo {

  @Component
  private BuildContext buildContext;

  /**
   * Includes of the files to be refreshed.
   */
  @Parameter
  private String[] includes;

  /**
   * Excludes of the files to be refreshed.
   */
  @Parameter
  private String[] excludes;

  @Parameter(defaultValue = "${basedir}")
  private String basedir;

  /**
   * Optional parameter. Will take all the files found (and refreshed) and copy them to the given base directory.
   */
  @Parameter
  private String[] copyToOutputs;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    DirectoryScanner scanner = new DirectoryScanner();
    scanner.setBasedir(basedir);
    if (includes != null)
      scanner.setIncludes(includes);
    if (excludes != null)
      scanner.setExcludes(excludes);

    scanner.scan();
    String[] refreshFiles = scanner.getIncludedFiles();
    getLog().info("Refreshing " + refreshFiles.length + " files.");
    if (getLog().isDebugEnabled())
      getLog().debug("Refreshing files: " + Arrays.asList(refreshFiles));

    for (String fileName : refreshFiles) {
      buildContext.refresh(new File(basedir, fileName));
    }

    if (copyToOutputs != null) {
      getLog().info("Copying " + refreshFiles.length + " files to " + Arrays.asList(copyToOutputs));
      for (String fileName : refreshFiles) {
        for (String copyToOutput : copyToOutputs) {
          File sourceFile = new File(basedir, fileName);
          File destFile = new File(copyToOutput, fileName);

          if (destFile.getParentFile() != null && !destFile.getParentFile().exists()) {
            if (!destFile.getParentFile().mkdirs())
              throw new MojoExecutionException("Could not create directory " + destFile.getParentFile());
          }

          try (FileInputStream fis = new FileInputStream(sourceFile)) {
            try (OutputStream fos = buildContext.newFileOutputStream(destFile)) {
              ByteStreams.copy(fis, fos);
            }
          } catch (IOException e) {
            throw new MojoExecutionException("Could not copy " + sourceFile + " to " + destFile);
          }
        }
      }
    }

    getLog().info("Done.");
  }
}