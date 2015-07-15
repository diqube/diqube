package org.diqube.util;

import java.util.Random;

import org.diqube.context.AutoInstatiate;

/**
 * Simple manager for random numbers.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class RandomManager {
  private Random random = new Random();

  /**
   * Return a random integer, see {@link Random#nextInt(int)}.
   */
  public int nextInt(int boundExclusive) {
    return random.nextInt(boundExclusive);
  }
}
