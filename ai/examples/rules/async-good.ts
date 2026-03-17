/**
 * @section types
 */

type SyncInvoicesCommand = { accountId: string };
type Invoice = { id: string; accountId: string };
type SyncResult = { saved: number };
type InvoiceSource = { fetch(accountId: string): Promise<Invoice[]> };
type InvoiceWriter = { persist(invoices: Invoice[]): Promise<SyncResult> };

/**
 * @section class
 */

export class InvoiceSyncService {
  /**
   * @section private:attributes
   */

  private readonly source: InvoiceSource;
  private readonly writer: InvoiceWriter;

  /**
   * @section constructor
   */

  public constructor(source: InvoiceSource, writer: InvoiceWriter) {
    this.source = source;
    this.writer = writer;
  }

  /**
   * @section factory
   */

  public static create(source: InvoiceSource, writer: InvoiceWriter): InvoiceSyncService {
    const service = new InvoiceSyncService(source, writer);
    return service;
  }

  /**
   * @section public:methods
   */

  public async execute(command: SyncInvoicesCommand): Promise<SyncResult> {
    const invoices: Invoice[] = await this.source.fetch(command.accountId);
    const result: SyncResult = await this.writer.persist(invoices);
    return result;
  }

  /**
   * @section static:methods
   */

  // empty
}
