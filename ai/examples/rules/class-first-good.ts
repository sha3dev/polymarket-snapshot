/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section consts
 */

const serviceName = "invoice-service";

/**
 * @section types
 */

type CreateInvoiceCommand = { customerId: string; amount: number };
type Invoice = { id: string; customerId: string; amount: number; createdAt: Date };

/**
 * @section class
 */

export class InvoiceService {
  /**
   * @section private:attributes
   */

  private readonly requestId: string;

  /**
   * @section public:properties
   */

  public readonly serviceName: string;

  /**
   * @section constructor
   */

  public constructor() {
    this.requestId = randomUUID();
    this.invoicesById = new Map<string, Invoice>();
    this.serviceName = serviceName;
  }

  /**
   * @section factory
   */

  public static create(): InvoiceService {
    const service = new InvoiceService();
    return service;
  }

  /**
   * @section private:methods
   */

  private toInvoice(command: CreateInvoiceCommand): Invoice {
    const invoice: Invoice = { id: randomUUID(), customerId: command.customerId, amount: command.amount, createdAt: new Date() };
    return invoice;
  }

  /**
   * @section public:methods
   */

  public async create(command: CreateInvoiceCommand): Promise<Invoice> {
    const invoice: Invoice = this.toInvoice(command);
    this.invoicesById.set(invoice.id, invoice);
    return invoice;
  }

  /**
   * @section static:methods
   */

  // empty
}
