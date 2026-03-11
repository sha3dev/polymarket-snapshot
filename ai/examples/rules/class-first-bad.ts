/**
 * @section types
 */

type CreateInvoiceCommand = { customerId: string; amount: number };
type Invoice = { id: string; customerId: string; amount: number };

export class InvoiceService {
  /**
   * @section factory
   */

  public static create(): InvoiceService {
    const service = new InvoiceService();
    return service;
  }

  /**
   * @section public:methods
   */

  public async create(command: CreateInvoiceCommand): Promise<Invoice> {
    if (!command.customerId) {
      return Promise.reject(new Error("invalid command"));
    }

    const invoice: Invoice = { id: "1", customerId: command.customerId, amount: command.amount };
    return invoice;
  }

  /**
   * @section static:methods
   */

  // empty
}
