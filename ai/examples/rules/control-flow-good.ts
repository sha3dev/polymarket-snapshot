/**
 * @section types
 */

type FeatureFlagMap = Record<string, boolean>;

/**
 * @section class
 */

export class FeatureGate {
  /**
   * @section private:attributes
   */

  private readonly flags: FeatureFlagMap;

  /**
   * @section constructor
   */

  public constructor(flags: FeatureFlagMap) {
    this.flags = flags;
  }

  /**
   * @section factory
   */

  public static from(flags: FeatureFlagMap): FeatureGate {
    const gate = new FeatureGate(flags);
    return gate;
  }

  /**
   * @section public:methods
   */

  public canRunTask(key: string): boolean {
    let enabled = false;

    if (this.flags[key] === true) {
      enabled = true;
    }

    return enabled;
  }

  /**
   * @section static:methods
   */

  // empty
}
