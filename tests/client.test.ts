import { describe, it, expect, afterEach, vi } from 'vitest'
import Fastify, { type FastifyInstance } from 'fastify'
import { createClient } from '../src/client'

let app: FastifyInstance

// Timers of the handlers that hang on purpose: they have to be cancelled or
// they keep the vitest process alive once the suite ends.
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
  if (typeof addr === 'string' || addr === null) throw new Error('no port')
  return `http://127.0.0.1:${addr.port}`
}

describe('createClient', () => {
  it('getJson decodes the response', async () => {
    const url = await serve(a => a.get('/api/v1/price', async () => ({ amount: 30 })))

    const out = await createClient(url).getJson<{ amount: number }>('/api/v1/price')

    expect(out.amount).toBe(30)
  })

  it('postJson sends the body and returns the response', async () => {
    const url = await serve(a =>
      a.post<{ Body: { seat: number } }>('/api/v1/seat/reserve', async req => ({ reserved: req.body.seat })),
    )

    const out = await createClient(url).postJson<{ reserved: number }>('/api/v1/seat/reserve', { seat: 7 })

    expect(out.reserved).toBe(7)
  })

  it('throws an error when the service answers 500', async () => {
    const url = await serve(a => a.get('/boom', async (_r, reply) => reply.code(500).send({})))

    await expect(createClient(url).getJson('/boom')).rejects.toThrow('500')
  })

  it('throws an error with the path when the 200 response is not JSON', async () => {
    const url = await serve(a =>
      a.get('/api/v1/raw', async (_req, reply) =>
        reply.header('content-type', 'text/plain').send('not json'),
      ),
    )

    await expect(createClient(url).getJson('/api/v1/raw')).rejects.toThrow('/api/v1/raw')
  })

  it('throws an error with the URL when the service does not answer', async () => {
    const url = await serve(a => a.get('/health', async () => ({ ok: true })))
    await app.close()

    await expect(createClient(url).getJson('/health')).rejects.toThrow(url)
  })

  // A service that ACCEPTS the connection and never answers is the
  // dangerous case: there is no ECONNREFUSED to cut the wait short, so
  // without an AbortSignal the `fetch` hangs forever and with it the request
  // that started it (gateway -> order -> seat is on the critical path). Here
  // the handler never answers and the client has to abort on its own.
  it('aborts when the service accepts the connection but does not answer', async () => {
    const url = await serve(a =>
      a.get('/hangs', () => new Promise(resolve => {
        pending.push(setTimeout(() => resolve({ late: true }), 30_000))
      })),
    )

    const started = Date.now()
    await expect(createClient(url, 200).getJson('/hangs')).rejects.toThrow(/did not answer in 200 ms/)
    expect(Date.now() - started).toBeLessThan(5_000)
  })

  it('the default timeout is 5 s, same as tt-lib-go and tt-lib-py', async () => {
    const url = await serve(a => a.get('/health', async () => ({ ok: true })))
    const spy = vi.spyOn(AbortSignal, 'timeout')

    await createClient(url).getJson('/health')

    expect(spy).toHaveBeenCalledWith(5_000)
    spy.mockRestore()
  })
})
