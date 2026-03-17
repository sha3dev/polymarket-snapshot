/**
 * @section types
 */

type PaymentInput = { amount: number; currency: string; metadata: Record<string, string> };
type PaymentDraft = { amount: number; currency: string; metadata: Record<string, string> };

/**
 * @section class
 */

export class PaymentNormalizer {
  /**
   * @section factory
   */

  public static create(): PaymentNormalizer {
    const normalizer = new PaymentNormalizer();
    return normalizer;
  }

  /**
   * @section private:methods
   */

  private normalizeAmount(amount: number): number {
    const normalizedAmount = Number(amount.toFixed(2));
    return normalizedAmount;
  }

  private normalizeCurrency(currency: string): string {
    const normalizedCurrency = currency.trim().toUpperCase();
    return normalizedCurrency;
  }

  private sanitizeMetadata(metadata: Record<string, string>): Record<string, string> {
    const sanitizedMetadata: Record<string, string> = { ...metadata };
    return sanitizedMetadata;
  }

  /**
   * @section public:methods
   */

  public normalize(input: PaymentInput): PaymentDraft {
    const amount: number = this.normalizeAmount(input.amount);
    const currency: string = this.normalizeCurrency(input.currency);
    const metadata: Record<string, string> = this.sanitizeMetadata(input.metadata);
    const draft: PaymentDraft = { amount, currency, metadata };
    return draft;
  }

  /**
   * @section static:methods
   */

  // empty
}
