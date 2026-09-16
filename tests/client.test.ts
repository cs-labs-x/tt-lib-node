import { describe, it, expect, afterEach, vi } from 'vitest'
import Fastify, { type FastifyInstance } from 'fastify'
import { createClient } from '../src/client'

let app: FastifyInstance

// Timers de los handlers que se cuelgan a propósito: hay que cancelarlos o
// mantienen vivo el proceso de vitest al terminar la suite.
const pending: ReturnType<typeof setTimeout>[] = []

afterEach(async () => {
  for (const timer of pending.splice(0)) clearTimeout(timer)
  await app?.close()
})

async function serve(handler: (app: FastifyInstance) => void): Promise<string> {
  app = Fastify()
  handler(app)
  await app.listen({ port: 0, host: '127.0.0.1' })
  const addr = app.server.address()
  if (typeof addr === 'string' || addr === null) throw new Error('sin puerto')
  return `http://127.0.0.1:${addr.port}`
}

describe('createClient', () => {
  it('getJson decodifica la respuesta', async () => {
    const url = await serve(a => a.get('/api/v1/price', async () => ({ amount: 30 })))

    const out = await createClient(url).getJson<{ amount: number }>('/api/v1/price')

    expect(out.amount).toBe(30)
  })

  it('postJson envía el cuerpo y devuelve la respuesta', async () => {
    const url = await serve(a =>
      a.post<{ Body: { seat: number } }>('/api/v1/seat/reserve', async req => ({ reserved: req.body.seat })),
    )

    const out = await createClient(url).postJson<{ reserved: number }>('/api/v1/seat/reserve', { seat: 7 })

    expect(out.reserved).toBe(7)
  })

  it('lanza error cuando el servicio responde 500', async () => {
    const url = await serve(a => a.get('/boom', async (_r, reply) => reply.code(500).send({})))

    await expect(createClient(url).getJson('/boom')).rejects.toThrow('500')
  })

  it('lanza error con la ruta cuando la respuesta 200 no es JSON', async () => {
    const url = await serve(a =>
      a.get('/api/v1/raw', async (_req, reply) =>
        reply.header('content-type', 'text/plain').send('no soy json'),
      ),
    )

    await expect(createClient(url).getJson('/api/v1/raw')).rejects.toThrow('/api/v1/raw')
  })

  it('lanza error con la URL cuando el servicio no responde', async () => {
    const url = await serve(a => a.get('/health', async () => ({ ok: true })))
    await app.close()

    await expect(createClient(url).getJson('/health')).rejects.toThrow(url)
  })

  // Un servicio que ACEPTA la conexión y no contesta es el caso peligroso:
  // no hay ECONNREFUSED que corte la espera, así que sin AbortSignal el
  // `fetch` se queda colgado para siempre y con él la petición que lo
  // originó (gateway -> order -> seat está en el camino crítico). Aquí el
  // handler nunca responde y el cliente tiene que abortar solo.
  it('aborta cuando el servicio acepta la conexión pero no responde', async () => {
    const url = await serve(a =>
      a.get('/cuelga', () => new Promise(resolve => {
        pending.push(setTimeout(() => resolve({ tarde: true }), 30_000))
      })),
    )

    const started = Date.now()
    await expect(createClient(url, 200).getJson('/cuelga')).rejects.toThrow(/no respondió en 200 ms/)
    expect(Date.now() - started).toBeLessThan(5_000)
  })

  it('el timeout por defecto son 5 s, igual que tt-lib-go y tt-lib-py', async () => {
    const url = await serve(a => a.get('/health', async () => ({ ok: true })))
    const spy = vi.spyOn(AbortSignal, 'timeout')

    await createClient(url).getJson('/health')

    expect(spy).toHaveBeenCalledWith(5_000)
    spy.mockRestore()
  })
})
