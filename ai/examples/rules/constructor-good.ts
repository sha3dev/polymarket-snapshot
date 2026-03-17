/**
 * @section consts
 */

const defaultInvoicePrefix = "INV";

/**
 * @section types
 */

type Clock = () => Date;
type IdFactory = () => string;

/**
 * @section class
 */

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

  public constructor(clock: Clock, idFactory: IdFactory, prefix = defaultInvoicePrefix) {
    this.clock = clock;
    this.idFactory = idFactory;
    this.prefix = prefix;
  }

  /**
   * @section factory
   */

  public static create(clock: Clock, idFactory: IdFactory): InvoiceIdBuilder {
    const builder = new InvoiceIdBuilder(clock, idFactory);
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
