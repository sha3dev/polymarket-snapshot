/**
 * @section consts
 */

const fallbackPrefix = "INV";

/**
 * @section types
 */

type Clock = () => Date;
type IdFactory = () => string;

export class InvoiceIdBuilder {
  /**
   * @section private:attributes
   */

  private readonly clock: Clock;
  private readonly idFactory: IdFactory;
  private readonly prefix: string;

  /**
   * @section constructor
   */

  public constructor() {
    // Bad: constructor wires concrete dependencies and environment details directly.
    this.clock = () => new Date();
    this.idFactory = () => Math.random().toString(36).slice(2, 8);
    this.prefix = process.env.INVOICE_PREFIX || fallbackPrefix;
  }

  /**
   * @section factory
   */

  public static create(): InvoiceIdBuilder {
    const builder = new InvoiceIdBuilder();
    return builder;
  }

  /**
   * @section private:methods
   */

  private currentYear(): number {
    const year = this.clock().getUTCFullYear();
    return year;
  }

  /**
   * @section public:methods
   */

  public build(): string {
    const rawId = this.idFactory();
    const year = this.currentYear();
    const invoiceId = `${this.prefix}-${year}-${rawId}`;
    return invoiceId;
  }

  /**
   * @section static:methods
   */

  // empty
}
