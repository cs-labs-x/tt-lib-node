import { describe, it, expect } from 'vitest'
import Fastify from 'fastify'
import { registerHealth } from '../src/health'

describe('registerHealth', () => {
  it('responde ok con el nombre del servicio', async () => {
    const app = Fastify()
    registerHealth(app, 'order-service')

    const res = await app.inject({ method: 'GET', url: '/health' })

    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual({ status: 'ok', service: 'order-service' })
  })
})
