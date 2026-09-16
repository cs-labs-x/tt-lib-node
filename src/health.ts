import type { FastifyInstance } from 'fastify'

/** Registra GET /health, común a todos los servicios Node. */
export function registerHealth(app: FastifyInstance, serviceName: string): void {
  app.get('/health', async () => ({ status: 'ok', service: serviceName }))
}
