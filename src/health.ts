import type { FastifyInstance } from 'fastify'

/** Registers GET /health, common to every Node service. */
export function registerHealth(app: FastifyInstance, serviceName: string): void {
  app.get('/health', async () => ({ status: 'ok', service: serviceName }))
}
