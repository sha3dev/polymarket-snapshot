/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import { InvalidInvoiceCommandError } from "./invoice.errors.ts";
import type { CreateInvoiceCommand, Invoice, InvoiceSummary } from "./invoice.types.ts";

/**
 * @section types
 */

// empty

/**
 * @section class
 */

export class InvoiceService {
  /**
   * @section private:attributes
   */

  private readonly invoicesById: Map<string, Invoice>;

  /**
   * @section constructor
   */

  public constructor() {
    this.invoicesById = new Map<string, Invoice>();
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

  private validate(command: CreateInvoiceCommand): void {
    if (!command.customerId.trim()) {
      throw InvalidInvoiceCommandError.forReason("customerId is required");
    }

    if (command.amount <= config.MINIMUM_INVOICE_AMOUNT) {
      throw InvalidInvoiceCommandError.forReason("amount must be greater than zero");
    }
  }

  private toInvoice(command: CreateInvoiceCommand): Invoice {
    const invoice: Invoice = { id: randomUUID(), customerId: command.customerId, amount: command.amount, issuedAt: new Date() };
    return invoice;
  }

  /**
   * @section public:methods
   */

  public async create(command: CreateInvoiceCommand): Promise<Invoice> {
    this.validate(command);
    const createdInvoice: Invoice = this.toInvoice(command);
    this.invoicesById.set(createdInvoice.id, createdInvoice);
    return createdInvoice;
  }

  public async summarizeForCustomer(customerId: string): Promise<InvoiceSummary> {
    const allInvoices: Invoice[] = Array.from(this.invoicesById.values());
    const invoices: Invoice[] = allInvoices.filter((invoice) => {
      return invoice.customerId === customerId;
    });
    const totalAmount = invoices.reduce((sum, invoice) => {
      return sum + invoice.amount;
    }, 0);
    const summary: InvoiceSummary = { count: invoices.length, totalAmount };
    return summary;
  }

  /**
   * @section static:methods
   */

  // empty
}
