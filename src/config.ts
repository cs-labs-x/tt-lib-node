export interface ServiceConfig {
  serviceName: string
  port: number
  databaseUrl?: string
  redisUrl?: string
}

/**
 * Lee una variable de entorno tratando la CADENA VACÍA COMO AUSENTE.
 *
 * Política unificada con las librerías hermanas: Go (`tt-lib-go/config`,
 * helper `env`, `if v := os.Getenv(key); v != ""`) y Python
 * (`tt_lib/config.py`, `os.getenv(...) or <default>`) ya la aplicaban; Node
 * no. `process.env.PORT ?? 8080` sólo cubre `undefined`, así que con
 * `PORT=""` — lo que produce un `environment: PORT:` sin valor en un
 * compose, o un `--env PORT=` — el `??` devolvía la cadena vacía y
 * `Number("")` es **0**: el puerto 0 significa "puerto efímero al azar" para
 * `listen()`, así que el servicio arrancaba sano pero en un puerto que nadie
 * conoce, y quedaba inalcanzable con el healthcheck en verde a nivel de
 * proceso. Una variable vacía es una variable no puesta.
 */
function env(key: string): string | undefined {
  const value = process.env[key]
  return value === undefined || value === '' ? undefined : value
}

/** Lee la configuración del entorno con valores por defecto de desarrollo. */
export function loadConfig(): ServiceConfig {
  return {
    serviceName: env('SERVICE_NAME') ?? 'unnamed-service',
    port: Number(env('PORT') ?? 8080),
    databaseUrl: env('DATABASE_URL'),
    redisUrl: env('REDIS_URL'),
  }
}
