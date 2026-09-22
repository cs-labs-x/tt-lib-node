export interface ServiceClient {
  getJson<T>(path: string): Promise<T>
  postJson<T>(path: string, body: unknown): Promise<T>
}

/**
 * Per-request timeout, in milliseconds.
 *
 * Same budget as the sibling libraries — Go uses
 * `http.Client{Timeout: 5 * time.Second}` (tt-lib-go/httpclient) and Python
 * `httpx.Client(timeout=5.0)` (tt_lib/client.py) — so that the three behave
 * the same way against a hung service.
 *
 * With no timeout, `fetch` waits forever: a single target that accepts the
 * connection and never answers blocks the caller forever and, with enough
 * requests, exhausts the Fastify pool. It sits on the critical path of the
 * booking vertical (gateway -> order -> seat, payment -> order), so a hang
 * in seat-service spread until it left the gateway with no capacity.
 */
const DEFAULT_TIMEOUT_MS = 5_000

/**
 * Creates a client against the base URL of another service.
 *
 * `timeoutMs` covers the WHOLE request (connection, headers and reading the
 * body): the `AbortSignal` goes to the `fetch`, and undici propagates it to
 * the body stream too, so a response that starts and then stalls halfway
 * aborts as well.
 */
export function createClient(baseUrl: string, timeoutMs: number = DEFAULT_TIMEOUT_MS): ServiceClient {
  function describe(target: string, cause: unknown): Error {
    if (cause instanceof Error && (cause.name === 'TimeoutError' || cause.name === 'AbortError')) {
      return new Error(`${target} did not answer in ${timeoutMs} ms`, { cause })
    }
    return new Error(`could not connect to ${target}`, { cause })
  }

  async function request<T>(path: string, init: RequestInit): Promise<T> {
    const target = baseUrl + path
    let res: Response
    try {
      res = await fetch(target, {
        ...init,
        signal: AbortSignal.timeout(timeoutMs),
        headers: { 'content-type': 'application/json', ...(init.headers ?? {}) },
      })
    } catch (cause) {
      throw describe(target, cause)
    }
    if (!res.ok) throw new Error(`${target} returned ${res.status}`)
    try {
      return (await res.json()) as T
    } catch (cause) {
      // The same signal aborts reading the body, so a body that stalls
      // halfway arrives here as a TimeoutError, not as invalid JSON: it has
      // to be told apart so we do not report "not JSON" when what really
      // happened is that the time ran out.
      if (cause instanceof Error && (cause.name === 'TimeoutError' || cause.name === 'AbortError')) {
        throw describe(target, cause)
      }
      throw new Error(`${target} returned a response that is not JSON`, { cause })
    }
  }

  return {
    getJson: <T>(path: string) => request<T>(path, { method: 'GET' }),
    postJson: <T>(path: string, body: unknown) =>
      request<T>(path, { method: 'POST', body: JSON.stringify(body) }),
  }
}
