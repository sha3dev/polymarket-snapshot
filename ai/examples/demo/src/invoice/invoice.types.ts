export type InvoiceId = string;

export type CustomerId = string;

export type Invoice = { id: InvoiceId; customerId: CustomerId; amount: number; issuedAt: Date };

export type CreateInvoiceCommand = { customerId: CustomerId; amount: number };

export type InvoiceSummary = { count: number; totalAmount: number };
