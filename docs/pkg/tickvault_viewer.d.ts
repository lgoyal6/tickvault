/* tslint:disable */
/* eslint-disable */

/**
 * A scrubbable partition: one venue, one symbol, one day.
 */
export class Viewer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Add one Parquet file's bytes, in archive order. Returns rows decoded.
     *
     * Files must arrive in the order the manifest lists them. The archive is
     * an append-only record of arrival, so out of order files would rebuild a
     * book that never existed.
     */
    add_file(bytes: Uint8Array): number;
    /**
     * The book after exactly `count` messages, which is what the scrubber
     * steps through when someone drags it one message at a time.
     */
    book_after(count: number, depth: number): string;
    /**
     * The book after the last message at or before `at_ns`.
     *
     * `at_ns` is a decimal string for the same precision reason.
     */
    book_at(at_ns: string, depth: number): string;
    /**
     * Receipt instant of message `i`, as a decimal string of nanoseconds.
     *
     * A string rather than a number because these are past 2^53, where a
     * double starts skipping integers, and the page turns them back into
     * BigInt.
     */
    message_at(i: number): string | undefined;
    /**
     * Messages in the partition.
     */
    messages(): number;
    /**
     * `feed_depth` is the window the venue's feed carried, taken from the
     * manifest. A rebuild has to truncate exactly as the recorder did or it
     * grows levels the feed had already dropped, and eventually crosses.
     */
    constructor(symbol: string, book_level: number, feed_depth?: number | null);
    rows(): number;
    /**
     * Index the rows into messages. Call once, after the last `add_file`.
     */
    seal(): void;
    /**
     * First and last receipt instants, as decimal nanosecond strings.
     */
    span(): any[];
}

/**
 * Turn a panic into something readable in the console rather than
 * `unreachable executed`.
 */
export function start(): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_viewer_free: (a: number, b: number) => void;
    readonly viewer_add_file: (a: number, b: number, c: number) => [number, number, number];
    readonly viewer_book_after: (a: number, b: number, c: number) => [number, number, number, number];
    readonly viewer_book_at: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly viewer_message_at: (a: number, b: number) => [number, number];
    readonly viewer_messages: (a: number) => number;
    readonly viewer_new: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly viewer_rows: (a: number) => number;
    readonly viewer_seal: (a: number) => void;
    readonly viewer_span: (a: number) => [number, number];
    readonly start: () => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __externref_drop_slice: (a: number, b: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
