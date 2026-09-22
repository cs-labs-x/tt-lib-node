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
  it('reads the configuration from the environment', () => {
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

  it('falls back to the defaults when the variables are not set', () => {
    for (const key of KEYS) unsetEnv(key)

    const cfg = loadConfig()

    expect(cfg.serviceName).toBe('unnamed-service')
    expect(cfg.port).toBe(8080)
    expect(cfg.databaseUrl).toBeUndefined()
    expect(cfg.redisUrl).toBeUndefined()
  })

  // Policy common to the three libraries (see the comment on `env` in
  // src/config.ts): an empty variable is treated as missing. Before,
  // `Number(process.env.PORT ?? 8080)` with PORT="" gave 0 — which for
  // `listen()` means "random ephemeral port", not 8080.
  it('treats an empty variable as missing, not as the value ""', () => {
    for (const key of KEYS) setEnv(key, '')

    const cfg = loadConfig()

    expect(cfg.port).toBe(8080)
    expect(cfg.serviceName).toBe('unnamed-service')
    expect(cfg.databaseUrl).toBeUndefined()
    expect(cfg.redisUrl).toBeUndefined()
  })

  it('an empty PORT never produces port 0 (a random port)', () => {
    setEnv('PORT', '')

    expect(loadConfig().port).not.toBe(0)
  })
})
