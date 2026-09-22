export interface ServiceClient {
  getJson<T>(path: string): Promise<T>
  postJson<T>(path: string, body: unknown): Promise<T>
}

/**
 * Timeout por petición, en milisegundos.
 *
 * Mismo presupuesto que las librerías hermanas — Go usa
 * `http.Client{Timeout: 5 * time.Second}` (tt-lib-go/httpclient) y Python
 * `httpx.Client(timeout=5.0)` (tt_lib/client.py) — para que las tres se
 * comporten igual ante un servicio colgado.
 *
 * Sin timeout, `fetch` espera indefinidamente: un solo destino que acepta la
 * conexión y no contesta bloquea al llamante para siempre y, con suficientes
 * peticiones, agota el pool de Fastify. Está en el camino crítico del
 * vertical de reserva (gateway -> order -> seat, payment -> order), así que
 * un cuelgue en seat-service se propagaba hasta dejar el gateway sin
 * capacidad.
 */
const DEFAULT_TIMEOUT_MS = 5_000

/**
 * Crea un cliente contra la URL base de otro servicio.
 *
 * `timeoutMs` cubre la petición ENTERA (conexión, cabeceras y lectura del
 * cuerpo): el `AbortSignal` va al `fetch`, y undici lo propaga también al
 * stream del cuerpo, así que una respuesta que empieza y se queda a medias
 * también aborta.
 */
export function createClient(baseUrl: string, timeoutMs: number = DEFAULT_TIMEOUT_MS): ServiceClient {
  function describe(target: string, cause: unknown): Error {
    if (cause instanceof Error && (cause.name === 'TimeoutError' || cause.name === 'AbortError')) {
      return new Error(`${target} no respondió en ${timeoutMs} ms`, { cause })
    }
    return new Error(`no se pudo conectar con ${target}`, { cause })
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
    if (!res.ok) throw new Error(`${target} devolvió ${res.status}`)
    try {
      return (await res.json()) as T
    } catch (cause) {
      // El mismo signal aborta la lectura del cuerpo, así que un cuerpo que
      // se queda a medias llega aquí como TimeoutError, no como JSON
      // inválido: hay que distinguirlo para no reportar "no es JSON" cuando
      // lo que pasó fue que se agotó el tiempo.
      if (cause instanceof Error && (cause.name === 'TimeoutError' || cause.name === 'AbortError')) {
        throw describe(target, cause)
      }
      throw new Error(`${target} devolvió una respuesta que no es JSON`, { cause })
    }
  }

  return {
    getJson: <T>(path: string) => request<T>(path, { method: 'GET' }),
    postJson: <T>(path: string, body: unknown) =>
      request<T>(path, { method: 'POST', body: JSON.stringify(body) }),
  }
}
