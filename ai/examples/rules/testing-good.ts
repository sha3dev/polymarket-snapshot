/**
 * @section consts
 */

const millisecondsPerDay = 24 * 60 * 60 * 1000;

/**
 * @section types
 */

type Invoice = { issuedAt: Date };
type EscalationDecision = "manual-review" | "no-escalation";

/**
 * @section class
 */

export class InvoiceEscalationPolicy {
  /**
   * @section factory
   */

  public static create(): InvoiceEscalationPolicy {
    const policy = new InvoiceEscalationPolicy();
    return policy;
  }

  /**
   * @section private:methods
   */

  private daysBetween(from: Date, to: Date): number {
    const diffInMilliseconds = to.getTime() - from.getTime();
    const dayCount = Math.floor(diffInMilliseconds / millisecondsPerDay);
    return dayCount;
  }

  /**
   * @section public:methods
   */

  // Business rule: invoices older than 30 days are escalated for manual review.
  public evaluateEscalation(invoice: Invoice, now: Date): EscalationDecision {
    const ageInDays: number = this.daysBetween(invoice.issuedAt, now);
    const decision: EscalationDecision = ageInDays > 30 ? "manual-review" : "no-escalation";
    return decision;
  }

  /**
   * @section static:methods
   */

  // empty
}
