/**
 * @section types
 */

// empty

export class ControlFlowPolicy {
  /**
   * @section factory
   */

  public static create(): ControlFlowPolicy {
    const policy = new ControlFlowPolicy();
    return policy;
  }

  /**
   * @section public:methods
   */

  public badSnippet(): string {
    const snippet = "if (isEnabled) executeTask();";
    return snippet;
  }

  /**
   * @section static:methods
   */

  // empty
}
