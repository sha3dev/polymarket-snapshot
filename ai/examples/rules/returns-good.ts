/**
 * @section types
 */

type InvoiceStatus = "paid" | "void" | "pending";

/**
 * @section class
 */

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
    let label: string;

    if (status === "paid") {
      label = "Paid";
    } else if (status === "void") {
      label = "Void";
    } else {
      label = "Pending";
    }

    return label;
  }

  /**
   * @section static:methods
   */

  // empty
}
