import Arweave from 'arweave';
import { CreateTransactionInterface } from 'arweave/node/common';
import Transaction, { TransactionInterface } from 'arweave/node/lib/transaction';
import { bufferTob64Url } from 'arweave/node/lib/utils';
import { JWKInterface } from 'arweave/node/lib/wallet';
import { pipeline } from 'stream/promises';
import { generateTransactionChunksAsync } from './generate-transaction-chunks-async';
import { backOff } from 'exponential-backoff';

/**
 * Creates an Arweave transaction from the piped data stream.
 */
export function createTransactionAsync(
  attributes: Partial<Omit<CreateTransactionInterface, 'data'>>,
  arweave: Arweave,
  jwk: JWKInterface | null | undefined,
) {
  return async (source: AsyncIterable<Buffer>): Promise<Transaction> => {
    const chunks = await pipeline(source, generateTransactionChunksAsync());

    const txAttrs = Object.assign({}, attributes);

    txAttrs.owner ??= jwk?.n;
    txAttrs.last_tx ??= await backOff(() => arweave.transactions.getTransactionAnchor(), { numOfAttempts: 10, startingDelay: 500 });

    const lastChunk = chunks.chunks[chunks.chunks.length - 1];
    const dataByteLength = lastChunk.maxByteRange;

    txAttrs.reward ??= await backOff(() => arweave.transactions.getPrice(dataByteLength, txAttrs.target), { numOfAttempts: 5, startingDelay: 500 });

    txAttrs.data_size = dataByteLength.toString();

    const tx = new Transaction(txAttrs as TransactionInterface);

    tx.chunks = chunks;
    tx.data_root = bufferTob64Url(chunks.data_root);

    return tx;
  };
}
