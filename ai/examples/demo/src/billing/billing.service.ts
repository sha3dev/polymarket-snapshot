/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { InvoiceService } from "../invoice/invoice.service.ts";
import type { InvoiceSummary } from "../invoice/invoice.types.ts";

/**
 * @section types
 */

export type BillingSnapshot = { customerId: string; invoiceCount: number; totalAmount: number; formattedTotal: string; statusServiceUrl: string };

/**
 * @section class
 */

export class BillingService {
  /**
   * @section private:attributes
   */

  private readonly invoiceService: InvoiceService;

  /**
   * @section constructor
   */

  public constructor(invoiceService: InvoiceService) {
    this.invoiceService = invoiceService;
  }

  /**
   * @section factory
   */

  public static create(invoiceService: InvoiceService): BillingService {
    const service = new BillingService(invoiceService);
    return service;
  }

  /**
   * @section private:methods
   */

  private formatCurrency(amount: number): string {
    const formattedAmount = `${config.BILLING_CURRENCY_SYMBOL}${amount.toFixed(2)}`;
    return formattedAmount;
  }

  /**
   * @section public:methods
   */

  public async snapshot(customerId: string): Promise<BillingSnapshot> {
    const summary: InvoiceSummary = await this.invoiceService.summarizeForCustomer(customerId);
    const snapshot: BillingSnapshot = {
      customerId,
      invoiceCount: summary.count,
      totalAmount: summary.totalAmount,
      formattedTotal: this.formatCurrency(summary.totalAmount),
      statusServiceUrl: config.STATUS_SERVICE_URL,
    };
    return snapshot;
  }

  /**
   * @section static:methods
   */

  // empty
}
