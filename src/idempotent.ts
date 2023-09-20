import { FibonacciStrategy, Backoff } from 'backoff';
import { delay } from 'highoutput-utilities';

export class TimeoutError extends Error {
  constructor() {
    super('Timeout');
  }
}

export class RequestExistsError extends Error {
  constructor() {
    super('Request exists');
  }
}

export type Request = {
  id: string;
} & (
  | {
      status: 'STARTED';
    }
  | {
      status: 'DONE';
      result: any;
    });

export interface IdempotentStore {
  get(id: string): Promise<Request | null>;
  set(
    id: string,
    params:
      | {
          status: 'STARTED';
        }
      | {
          status: 'DONE';
          result: any;
        }
  ): Promise<boolean>;
}

export class Idempotent {
  constructor(
    private readonly store: IdempotentStore,
    private readonly options: { timeout?: string | number } = {}
  ) {}

  async execute<T = any>(fn: () => Promise<T>, request: string): Promise<T> {
    try {
      await this.store.set(request, { status: 'STARTED' });
    } catch (err) {
      if (!(err instanceof RequestExistsError)) {
        throw err;
      }

      return new Promise((resolve, reject) => {
        const handler = async () => {
          const requestDocument = await this.store.get(request);

          if (requestDocument && requestDocument.status === 'DONE') {
            resolve(requestDocument.result);
            return;
          }

          backoff.backoff();
        };

        const backoff = new Backoff(
          new FibonacciStrategy({
            initialDelay: 1,
            maxDelay: 100,
            randomisationFactor: 0.5,
          })
        );

        backoff.on('backoff', handler);

        handler();

        delay(this.options.timeout || '1m').then(() => {
          backoff.removeAllListeners();
          reject(new TimeoutError());
        });
      });
    }

    const result = await fn();

    await this.store.set(request, { status: 'DONE', result });
    return result;
  }
}

export default Idempotent;
