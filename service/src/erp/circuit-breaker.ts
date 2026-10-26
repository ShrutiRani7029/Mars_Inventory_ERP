export type BreakerState = 'closed' | 'open' | 'half-open';

/**
 * Stops hammering the ERP during its Sunday maintenance. After `threshold` consecutive
 * failures it opens; after `openMs` it lets exactly one probe through (half-open).
 */
export class CircuitBreaker {
  private failures = 0;
  private openedAt = 0;
  private probing = false;
  state: BreakerState = 'closed';

  constructor(private readonly threshold: number, private readonly openMs: number, private readonly now = () => Date.now()) {}

  allow(): boolean {
    if (this.state === 'closed') return true;
    if (this.state === 'open' && this.now() - this.openedAt >= this.openMs) {
      this.state = 'half-open';
      this.probing = false;
    }
    if (this.state === 'half-open' && !this.probing) {
      this.probing = true;
      return true;
    }
    return false;
  }

  success() {
    this.failures = 0;
    this.probing = false;
    this.state = 'closed';
  }

  failure() {
    this.failures += 1;
    this.probing = false;
    if (this.state === 'half-open' || this.failures >= this.threshold) {
      this.state = 'open';
      this.openedAt = this.now();
    }
  }
}
