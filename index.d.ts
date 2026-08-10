/**
 * Type definitions for the k6/x/iroh extension (go-iroh-perflab).
 * Field names use the k6 Go-to-JS mapping (snake_case for result
 * fields); option objects use the Go struct json tags (camelCase).
 */
declare module 'k6/x/iroh' {
  export interface Config {
    /** Endpoint ticket or iroh-blobs ticket of the peer to dial. */
    target: string;
    /** Connection ALPN; defaults to "perflab/0" ("/iroh-bytes/4" for blobs tickets). */
    alpn?: string;
    /** "vu" (default) or "shared". */
    endpointScope?: string;
    /** Cell label stamped as the peer tag on every sample ("go", "rust", ...). */
    peer?: string;
    /** "default", "disabled", or "forced" (requires relayURL). */
    relayMode?: string;
    relayURL?: string;
  }

  export interface StreamOpts {
    streams?: number;
    bytes?: number;
    msgSize?: number;
    timeoutMs?: number;
  }
  export interface StreamResult {
    streams: number;
    completed: number;
    bytes_sent: number;
    error?: string;
  }

  export interface DatagramOpts {
    count?: number;
    size?: number;
    timeoutMs?: number;
  }
  export interface DatagramResult {
    sent: number;
    echoed: number;
    lost: number;
    error?: string;
  }

  export interface FetchOpts {
    timeoutMs?: number;
    /** Fail when no bytes arrive for this long (wedge gate). */
    stallMs?: number;
  }
  export interface FetchResult {
    bytes: number;
    completed: boolean;
    stalled: boolean;
    /** Child blobs fetched for a collection (HashSeq) ticket; 0 for raw. */
    entries: number;
    error?: string;
  }

  export interface GossipOpts {
    count?: number;
    msgSize?: number;
    intervalMs?: number;
    timeoutMs?: number;
    /** Topic name; must match the gossip-member's -gossip-topic. */
    topic?: string;
  }
  export interface GossipResult {
    sent: number;
    echoed: number;
    lost: number;
    error?: string;
  }

  export interface RequestOpts {
    bytes?: number;
    timeoutMs?: number;
  }
  export interface RequestResult {
    sent: number;
    received: number;
    error?: string;
  }

  export class Client {
    constructor(config: Config);
    dial(): void;
    sendStreams(opts?: StreamOpts): StreamResult;
    echoDatagrams(opts?: DatagramOpts): DatagramResult;
    fetchBlob(opts?: FetchOpts): FetchResult;
    gossip(opts?: GossipOpts): GossipResult;
    request(opts?: RequestOpts): RequestResult;
    metricsSnapshot(): Record<string, number>;
    close(): void;
  }

  const iroh: { Client: typeof Client };
  export default iroh;
}
