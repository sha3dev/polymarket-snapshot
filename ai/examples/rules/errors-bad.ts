/**
 * @section types
 */

// empty

export class InvoiceLookup {
  /**
   * @section factory
   */

  public static create(): InvoiceLookup {
    const lookup = new InvoiceLookup();
    return lookup;
  }

  /**
   * @section public:methods
   */

  public ensureInvoiceExists(invoiceId: string, exists: boolean): void {
    try {
      if (!exists) {
        throw "missing";
      }
    } catch {
      // bad: error is silently swallowed
    }

    if (!exists) {
      throw new Error("failed");
    }

    console.log(invoiceId);
  }

  /**
   * @section static:methods
   */

  // empty
}
