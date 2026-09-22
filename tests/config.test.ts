import { describe, it, expect, afterEach } from 'vitest'
import { loadConfig } from '../src/config'

const KEYS = ['SERVICE_NAME', 'PORT', 'DATABASE_URL', 'REDIS_URL'] as const
const saved = new Map<string, string | undefined>()

afterEach(() => {
  for (const key of KEYS) {
    const before = saved.get(key)
    if (before === undefined) delete process.env[key]
    else process.env[key] = before
    saved.delete(key)
  }
})

function setEnv(key: (typeof KEYS)[number], value: string): void {
  if (!saved.has(key)) saved.set(key, process.env[key])
  process.env[key] = value
}

function unsetEnv(key: (typeof KEYS)[number]): void {
  if (!saved.has(key)) saved.set(key, process.env[key])
  delete process.env[key]
}

describe('loadConfig', () => {
  it('lee la configuración del entorno', () => {
    setEnv('SERVICE_NAME', 'order-service')
    setEnv('PORT', '8080')
    setEnv('DATABASE_URL', 'mysql://tt:tt@mysql:3306/order')
    setEnv('REDIS_URL', 'redis://redis:6379')

    const cfg = loadConfig()

    expect(cfg.serviceName).toBe('order-service')
    expect(cfg.port).toBe(8080)
    expect(cfg.databaseUrl).toBe('mysql://tt:tt@mysql:3306/order')
    expect(cfg.redisUrl).toBe('redis://redis:6379')
  })

  it('cae a los valores por defecto cuando las variables no están', () => {
    for (const key of KEYS) unsetEnv(key)

    const cfg = loadConfig()

    expect(cfg.serviceName).toBe('unnamed-service')
    expect(cfg.port).toBe(8080)
    expect(cfg.databaseUrl).toBeUndefined()
    expect(cfg.redisUrl).toBeUndefined()
  })

  // Política común a las tres librerías (ver el comentario de `env` en
  // src/config.ts): una variable vacía se trata como ausente. Antes,
  // `Number(process.env.PORT ?? 8080)` con PORT="" daba 0 — que para
  // `listen()` significa "puerto efímero al azar", no 8080.
  it('trata una variable vacía como ausente, no como el valor ""', () => {
    for (const key of KEYS) setEnv(key, '')

    const cfg = loadConfig()

    expect(cfg.port).toBe(8080)
    expect(cfg.serviceName).toBe('unnamed-service')
    expect(cfg.databaseUrl).toBeUndefined()
    expect(cfg.redisUrl).toBeUndefined()
  })

  it('PORT vacío nunca produce el puerto 0 (puerto al azar)', () => {
    setEnv('PORT', '')

    expect(loadConfig().port).not.toBe(0)
  })
})
