/**
 * @section types
 */

// empty

/**
 * @section class
 */

export class InvalidInvoiceCommandError extends Error {
  /**
   * @section private:attributes
   */

  private readonly reason: string;

  /**
   * @section constructor
   */

  public constructor(reason: string) {
    super(`Invalid invoice command: ${reason}`);
    this.name = "InvalidInvoiceCommandError";
    this.reason = reason;
  }

  /**
   * @section factory
   */

  public static forReason(reason: string): InvalidInvoiceCommandError {
    const error = new InvalidInvoiceCommandError(reason);
    return error;
  }

  /**
   * @section public:methods
   */

  public getReason(): string {
    const value = this.reason;
    return value;
  }

  /**
   * @section static:methods
   */

  // empty
}
