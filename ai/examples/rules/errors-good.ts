/**
 * @section types
 */

// empty

/**
 * @section class
 */

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
    if (!exists) {
      throw new Error(`Invoice not found: invoiceId=${invoiceId}`);
    }
  }

  /**
   * @section static:methods
   */

  // empty
}
