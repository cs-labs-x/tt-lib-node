import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  splitChannel,
  createRecorder,
  createConsumer,
  createPublisher,
  memoizeAsync,
  RETRY_DELAY_MS,
} from '../src/events'

// `createConsumer` arranca kafkajs de verdad si no se sustituye: en este
// archivo no hay broker, así que el único test que usa createConsumer
// necesita una conexión que se pueda dejar colgada a propósito, no una que
// falle contra un host inalcanzable. `vi.mock` está fuera de cualquier
// `describe`/`it` a propósito — vitest lo hoisting por encima de los
// imports, y su alcance es el módulo entero, así que aplica al archivo
// completo. No afecta al resto de los tests: ninguno construye un
// `createPublisher`, y `createConsumer` es el único que toca kafkajs.
//
// `connectBehaviour` deja que cada test decida qué hace `connect()`: por
// defecto se queda colgado (el caso que prueba que `start()` no bloquea), y
// el test del reintento lo cambia para fallar unas cuantas veces.
const kafkaSpies = {
  consumersCreated: 0,
  connectCalls: 0,
  connectBehaviour: (): Promise<void> => new Promise<void>(() => {}), // nunca resuelve
  reset(): void {
    this.consumersCreated = 0
    this.connectCalls = 0
    this.connectBehaviour = () => new Promise<void>(() => {})
  },
}

vi.mock('kafkajs', () => {
  class FakeConsumer {
    // `events` y `on` los usa createConsumer para saber cuándo se cae el
    // runner: sin ellos la sesión no sabría nunca que terminó.
    events = { CRASH: 'consumer.crash', STOP: 'consumer.stop' }
    on = vi.fn()
    connect = vi.fn(() => {
      kafkaSpies.connectCalls++
      return kafkaSpies.connectBehaviour()
    })
    subscribe = vi.fn(async () => {})
    run = vi.fn(async () => {})
    disconnect = vi.fn(async () => {})
  }
  class FakeKafka {
    consumer(): FakeConsumer {
      kafkaSpies.consumersCreated++
      return new FakeConsumer()
    }
    producer(): unknown {
      return { connect: vi.fn(), send: vi.fn(), disconnect: vi.fn() }
    }
  }
  return { Kafka: FakeKafka }
})

// amqplib tampoco se toca de verdad: no hay broker en este archivo. El fake
// permite además provocar lo que el bug de producción provocaba —que la
// conexión se cierre bajo los pies del publicador— y comprobar que la
// siguiente publicación vuelve a marcar. Como en el fake de kafkajs, las
// clases se definen DENTRO de la factoría (vitest la hoista por encima de los
// imports) y solo los métodos leen `amqpSpies`, que para entonces ya existe.
interface FakeAmqpConnection {
  emitClose(): void
  channel: { emitClose(): void; sent: Array<{ queue: string; body: Buffer }> }
}

const amqpSpies = {
  connects: 0,
  fail: false,
  connections: [] as FakeAmqpConnection[],
  reset(): void {
    this.connects = 0
    this.fail = false
    this.connections = []
  },
}

vi.mock('amqplib', () => {
  class Emitter {
    private handlers: Record<string, Array<() => void>> = {}
    on(event: string, fn: () => void): this {
      ;(this.handlers[event] ??= []).push(fn)
      return this
    }
    fire(event: string): void {
      for (const fn of this.handlers[event] ?? []) fn()
    }
  }
  class FakeChannel extends Emitter {
    sent: Array<{ queue: string; body: Buffer }> = []
    async assertQueue(): Promise<void> {}
    sendToQueue(queue: string, body: Buffer): void {
      this.sent.push({ queue, body })
    }
    async close(): Promise<void> {
      this.fire('close')
    }
    emitClose(): void {
      this.fire('close')
    }
  }
  class FakeConnection extends Emitter {
    channel = new FakeChannel()
    async createChannel(): Promise<FakeChannel> {
      return this.channel
    }
    async close(): Promise<void> {
      this.fire('close')
    }
    emitClose(): void {
      this.fire('close')
    }
  }
  return {
    default: {
      connect: async () => {
        amqpSpies.connects++
        if (amqpSpies.fail) throw new Error('rabbitmq no está listo')
        const conn = new FakeConnection()
        amqpSpies.connections.push(conn as unknown as FakeAmqpConnection)
        return conn
      },
    },
  }
})

// mysql2 igual: interesa contar CUÁNTOS pools se crean, que es lo que medía
// la fuga.
const mysqlSpies = {
  poolsCreated: 0,
  createTableFailures: 0,
  inserts: 0,
  reset(): void {
    this.poolsCreated = 0
    this.createTableFailures = 0
    this.inserts = 0
  },
}

vi.mock('mysql2/promise', () => ({
  default: {
    createPool: () => {
      mysqlSpies.poolsCreated++
      return {
        query: async (sql: string) => {
          if (sql.includes('CREATE TABLE')) {
            if (mysqlSpies.createTableFailures > 0) {
              mysqlSpies.createTableFailures--
              throw new Error('mysql todavía no acepta conexiones')
            }
            return [[], []]
          }
          mysqlSpies.inserts++
          return [[], []]
        },
        end: async () => {},
      }
    },
  },
}))

describe('splitChannel', () => {
  it('separa el transporte del nombre', () => {
    expect(splitChannel('kafka:order-events')).toEqual({ transport: 'kafka', name: 'order-events' })
    expect(splitChannel('rabbitmq:sms.send')).toEqual({ transport: 'rabbitmq', name: 'sms.send' })
  })

  it('deja el transporte vacío si el canal no lleva prefijo', () => {
    expect(splitChannel('order-events').transport).toBe('')
  })
})

describe('createRecorder', () => {
  it('registra en el log y no falla cuando no hay base de datos', async () => {
    const record = createRecorder('invoice-service', undefined)
    await expect(record('kafka:order-events', Buffer.from('{"a":1}'))).resolves.toBeUndefined()
  })

  it('registra en el log cuando el motor no es SQL soportado', async () => {
    const record = createRecorder('catalog-sync-service', 'mongodb://mongo:27017/catalog')
    await expect(record('kafka:travel-events', Buffer.from('{}'))).resolves.toBeUndefined()
  })
})

describe('memoizeAsync', () => {
  it('no cachea el fallo: una llamada posterior reintenta la operación', async () => {
    // Es exactamente el escenario que rompió la versión inicial de
    // tt-lib-go: la primera llamada falla (broker/DB aún no listos), y la
    // memoización no debe dejar el helper inutilizado para siempre.
    let calls = 0
    const op = memoizeAsync(async () => {
      calls++
      if (calls === 1) throw new Error('fallo transitorio')
      return 'listo'
    })

    await expect(op()).rejects.toThrow('fallo transitorio')
    await expect(op()).resolves.toBe('listo')
    expect(calls).toBe(2)
  })

  it('dos llamadas concurrentes comparten la misma promesa en curso', async () => {
    let calls = 0
    let resolveOp!: (value: string) => void
    const op = memoizeAsync(() => {
      calls++
      return new Promise<string>((resolve) => {
        resolveOp = resolve
      })
    })

    const first = op()
    const second = op()
    resolveOp('valor')

    await expect(first).resolves.toBe('valor')
    await expect(second).resolves.toBe('valor')
    expect(calls).toBe(1)
  })
})

describe('createConsumer', () => {
  beforeEach(() => {
    kafkaSpies.reset()
  })

  it('start() devuelve sin esperar a que la conexión resuelva', async () => {
    // El fake de kafkajs de arriba nunca resuelve `connect()`. Si `start()`
    // esperase la conexión en vez de solo dispararla, este `await` colgaría
    // hasta el timeout del test y fallaría — es la prueba de que no bloquea.
    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await expect(consumer.start()).resolves.toBeUndefined()
  }, 1000)

  it('reintenta el canal cuando la conexión falla, en vez de rendirse', async () => {
    // La regresión de la Task 7: al conectar de verdad, el broker devolvió
    // GroupCoordinatorNotAvailable, kafkajs tumbó el runner y el consumidor
    // se quedaba parado PARA SIEMPRE — el servicio seguía sano y sirviendo
    // /health sin volver a consumir un mensaje. Go y Python sí reintentaban.
    // Aquí `connect()` falla siempre: lo que se exige es que se vuelva a
    // intentar, no que acabe conectando.
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker no listo'))

    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()

    // RETRY_DELAY_MS entre intentos: con esperar algo más de dos ciclos basta
    // para distinguir "reintenta" de "se rindió tras el primero".
    await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * 2 + 500))
    const attempts = kafkaSpies.connectCalls
    await consumer.close()

    expect(attempts).toBeGreaterThan(1)
  }, 10_000)

  it('close() para el bucle: no se reintenta después de cerrar', async () => {
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker no listo'))

    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()
    await consumer.close()

    const afterClose = kafkaSpies.connectCalls
    await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * 2))
    expect(kafkaSpies.connectCalls).toBe(afterClose)
  }, 10_000)
})

describe('createPublisher sobre rabbitmq', () => {
  beforeEach(() => {
    amqpSpies.reset()
  })

  it('reutiliza el canal mientras la conexión siga viva', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(1)
    expect(amqpSpies.connections[0].channel.sent).toHaveLength(2)
  })

  // EL bug: `connectRabbit` estaba envuelto en `memoizeAsync`, que solo suelta
  // el memo cuando la promesa RECHAZA. Una vez resuelta, el canal quedaba
  // cacheado para siempre, y amqplib —a diferencia de aio_pika.connect_robust
  // en Python y del redial por publicación en Go— no se autorrepara: tras
  // reiniciar el broker, cada publicación fallaba sobre un canal muerto
  // indefinidamente, hasta reiniciar el contenedor. 8 de los 9 publicadores de
  // RabbitMQ del sistema son Node.
  it('vuelve a conectar tras un reinicio del broker', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    expect(amqpSpies.connects).toBe(1)

    // El broker se reinicia: amqplib emite 'close' sobre la conexión.
    amqpSpies.connections[0].emitClose()

    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(2)
    expect(amqpSpies.connections[1].channel.sent).toHaveLength(1)
  })

  it('vuelve a conectar también si lo que se cierra es el canal, no la conexión', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    amqpSpies.connections[0].channel.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(2)
  })

  it('no cachea el fallo de conexión: la siguiente publicación reintenta', async () => {
    amqpSpies.fail = true
    const publisher = createPublisher('food-service')
    await expect(publisher.publish('rabbitmq:food.deliver', {})).rejects.toThrow()

    amqpSpies.fail = false
    await expect(publisher.publish('rabbitmq:food.deliver', {})).resolves.toBeUndefined()
    expect(amqpSpies.connects).toBe(2)
  })

  it('un cierre viejo no tira la conexión que ya lo reemplazó', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    const primera = amqpSpies.connections[0]
    primera.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })

    // El 'close' rezagado de la conexión vieja no debe invalidar la nueva.
    primera.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 3 })
    expect(amqpSpies.connects).toBe(2)
  })
})

describe('createRecorder sobre mysql', () => {
  beforeEach(() => {
    mysqlSpies.reset()
  })

  // El pool se construía DENTRO del memo, así que cada intento fallido del
  // CREATE TABLE —el caso normal cuando compose arranca MySQL y el servicio a
  // la vez— dejaba un pool abandonado sin cerrar. Go cierra su conexión al
  // fallar y Python cierra el pool de asyncpg; Node con MySQL era el único que
  // lo fugaba.
  it('no crea un pool por reintento cuando el CREATE TABLE falla', async () => {
    mysqlSpies.createTableFailures = 2
    const record = createRecorder('food-delivery-service', 'mysql://tt:tt@mysql:3306/food-delivery')
    const payload = Buffer.from('{}')

    await expect(record('rabbitmq:food.deliver', payload)).rejects.toThrow()
    await expect(record('rabbitmq:food.deliver', payload)).rejects.toThrow()
    await expect(record('rabbitmq:food.deliver', payload)).resolves.toBeUndefined()

    expect(mysqlSpies.poolsCreated).toBe(1)
    expect(mysqlSpies.inserts).toBe(1)
  })

  it('memoiza el CREATE TABLE: no lo repite en cada mensaje', async () => {
    const record = createRecorder('food-delivery-service', 'mysql://tt:tt@mysql:3306/food-delivery')
    for (let i = 0; i < 3; i++) await record('rabbitmq:food.deliver', Buffer.from('{}'))
    expect(mysqlSpies.poolsCreated).toBe(1)
    expect(mysqlSpies.inserts).toBe(3)
  })
})

describe('la espera de reintento del consumidor es cancelable', () => {
  beforeEach(() => {
    kafkaSpies.reset()
  })

  // El comentario de `delay` prometía "o menos si `stopped()` pasa a true
  // mientras espera" y era mentira: no recibía ninguna señal de parada. Con
  // los servicios cerrando de forma ordenada eso se nota, porque `close()`
  // espera a los bucles y cada canal dormido añadía hasta RETRY_DELAY_MS al
  // cierre del servicio.
  it('close() despierta la espera en vez de aguantar RETRY_DELAY_MS', async () => {
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker no listo'))
    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()

    // Margen para que el primer intento falle y el bucle entre a dormir.
    await new Promise((r) => setTimeout(r, 200))

    const inicio = Date.now()
    await consumer.close()
    expect(Date.now() - inicio).toBeLessThan(RETRY_DELAY_MS / 2)
  }, 10_000)
})
