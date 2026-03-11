/**
 * @section types
 */

type Invoice = { issuedAt: Date };

export class InvoiceEscalationPolicy {
  /**
   * @section factory
   */

  public static create(): InvoiceEscalationPolicy {
    const policy = new InvoiceEscalationPolicy();
    return policy;
  }

  /**
   * @section public:methods
   */

  public evaluateEscalation(invoice: Invoice, now: Date): Promise<string> {
    return Promise.resolve(invoice).then((current: Invoice) => {
      const hasAge = now.getTime() - current.issuedAt.getTime() > 0;
      const decision = hasAge ? "x" : "y";
      return decision;
    });
  }

  /**
   * @section static:methods
   */

  // empty
}
