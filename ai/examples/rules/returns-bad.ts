/**
 * @section types
 */

type InvoiceStatus = "paid" | "void" | "pending";

export class InvoiceStatusPresenter {
  /**
   * @section factory
   */

  public static create(): InvoiceStatusPresenter {
    const presenter = new InvoiceStatusPresenter();
    return presenter;
  }

  /**
   * @section public:methods
   */

  public toStatusLabel(status: InvoiceStatus): string {
    if (status === "paid") {
      return "Paid";
    }

    if (status === "void") {
      return "Void";
    }

    return "Pending";
  }

  /**
   * @section static:methods
   */

  // empty
}
