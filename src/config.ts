export interface ServiceConfig {
  serviceName: string
  port: number
  databaseUrl?: string
  redisUrl?: string
}

/**
 * Reads an environment variable treating the EMPTY STRING AS MISSING.
 *
 * Policy unified with the sibling libraries: Go (`tt-lib-go/config`, `env`
 * helper, `if v := os.Getenv(key); v != ""`) and Python
 * (`tt_lib/config.py`, `os.getenv(...) or <default>`) already applied it;
 * Node did not. `process.env.PORT ?? 8080` only covers `undefined`, so with
 * `PORT=""` — which is what an `environment: PORT:` with no value in a
 * compose file, or a `--env PORT=`, produces — the `??` returned the empty
 * string and `Number("")` is **0**: port 0 means "random ephemeral port" for
 * `listen()`, so the service started up healthy but on a port that nobody
 * knows, and it stayed unreachable with the healthcheck green at the process
 * level. An empty variable is a variable that is not set.
 */
function env(key: string): string | undefined {
  const value = process.env[key]
  return value === undefined || value === '' ? undefined : value
}

/** Reads the configuration from the environment with development defaults. */
export function loadConfig(): ServiceConfig {
  return {
    serviceName: env('SERVICE_NAME') ?? 'unnamed-service',
    port: Number(env('PORT') ?? 8080),
    databaseUrl: env('DATABASE_URL'),
    redisUrl: env('REDIS_URL'),
  }
}
