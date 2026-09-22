import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  splitChannel,
  createRecorder,
  createConsumer,
  createPublisher,
  memoizeAsync,
  RETRY_DELAY_MS,
} from '../src/events'

// `createConsumer` really starts kafkajs if it is not replaced: in this
// file there is no broker, so the only test that uses createConsumer needs
// a connection that can be left hanging on purpose, not one that fails
// against an unreachable host. `vi.mock` is outside any `describe`/`it` on
// purpose — vitest hoists it above the imports, and its scope is the whole
// module, so it applies to the whole file. It does not affect the rest of
// the tests: none of them builds a `createPublisher`, and `createConsumer`
// is the only one that touches kafkajs.
//
// `connectBehaviour` lets each test decide what `connect()` does: by
// default it hangs (the case that proves `start()` does not block), and the
// retry test changes it to fail a few times.
const kafkaSpies = {
  consumersCreated: 0,
  connectCalls: 0,
  connectBehaviour: (): Promise<void> => new Promise<void>(() => {}), // never resolves
  reset(): void {
    this.consumersCreated = 0
    this.connectCalls = 0
    this.connectBehaviour = () => new Promise<void>(() => {})
  },
}

vi.mock('kafkajs', () => {
  class FakeConsumer {
    // createConsumer uses `events` and `on` to know when the runner goes
    // down: without them the session would never know that it ended.
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

// amqplib is not really touched either: there is no broker in this file.
// The fake also makes it possible to trigger what the production bug
// triggered —the connection closing under the publisher's feet— and to
// check that the next publish dials again. As in the kafkajs fake, the
// classes are defined INSIDE the factory (vitest hoists it above the
// imports) and only the methods read `amqpSpies`, which by then exists.
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
        if (amqpSpies.fail) throw new Error('rabbitmq is not ready')
        const conn = new FakeConnection()
        amqpSpies.connections.push(conn as unknown as FakeAmqpConnection)
        return conn
      },
    },
  }
})

// mysql2 the same: what matters is counting HOW MANY pools are created,
// which is what measured the leak.
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
              throw new Error('mysql is not accepting connections yet')
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
  it('splits the transport off the name', () => {
    expect(splitChannel('kafka:order-events')).toEqual({ transport: 'kafka', name: 'order-events' })
    expect(splitChannel('rabbitmq:sms.send')).toEqual({ transport: 'rabbitmq', name: 'sms.send' })
  })

  it('leaves the transport empty if the channel carries no prefix', () => {
    expect(splitChannel('order-events').transport).toBe('')
  })
})

describe('createRecorder', () => {
  it('writes to the log and does not fail when there is no database', async () => {
    const record = createRecorder('invoice-service', undefined)
    await expect(record('kafka:order-events', Buffer.from('{"a":1}'))).resolves.toBeUndefined()
  })

  it('writes to the log when the engine is not supported SQL', async () => {
    const record = createRecorder('catalog-sync-service', 'mongodb://mongo:27017/catalog')
    await expect(record('kafka:travel-events', Buffer.from('{}'))).resolves.toBeUndefined()
  })
})

describe('memoizeAsync', () => {
  it('does not cache the failure: a later call retries the operation', async () => {
    // It is exactly the scenario that broke the first version of
    // tt-lib-go: the first call fails (broker/DB not ready yet), and the
    // memoization must not leave the helper unusable forever.
    let calls = 0
    const op = memoizeAsync(async () => {
      calls++
      if (calls === 1) throw new Error('transient failure')
      return 'ready'
    })

    await expect(op()).rejects.toThrow('transient failure')
    await expect(op()).resolves.toBe('ready')
    expect(calls).toBe(2)
  })

  it('two concurrent calls share the same promise in flight', async () => {
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
    resolveOp('value')

    await expect(first).resolves.toBe('value')
    await expect(second).resolves.toBe('value')
    expect(calls).toBe(1)
  })
})

describe('createConsumer', () => {
  beforeEach(() => {
    kafkaSpies.reset()
  })

  it('start() returns without waiting for the connection to resolve', async () => {
    // The kafkajs fake above never resolves `connect()`. If `start()` waited
    // for the connection instead of only firing it, this `await` would hang
    // until the test timeout and fail — it is the proof that it does not
    // block.
    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await expect(consumer.start()).resolves.toBeUndefined()
  }, 1000)

  it('retries the channel when the connection fails, instead of giving up', async () => {
    // The Task 7 regression: on a real connect, the broker returned
    // GroupCoordinatorNotAvailable, kafkajs brought the runner down and the
    // consumer stayed stopped FOREVER — the service stayed healthy and
    // serving /health without consuming another message. Go and Python did
    // retry. Here `connect()` always fails: what is required is that it
    // tries again, not that it ends up connecting.
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker not ready'))

    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()

    // RETRY_DELAY_MS between attempts: waiting a bit more than two cycles is
    // enough to tell "it retries" apart from "it gave up after the first".
    await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * 2 + 500))
    const attempts = kafkaSpies.connectCalls
    await consumer.close()

    expect(attempts).toBeGreaterThan(1)
  }, 10_000)

  it('close() stops the loop: there is no retry after closing', async () => {
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker not ready'))

    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()
    await consumer.close()

    const afterClose = kafkaSpies.connectCalls
    await new Promise((r) => setTimeout(r, RETRY_DELAY_MS * 2))
    expect(kafkaSpies.connectCalls).toBe(afterClose)
  }, 10_000)
})

describe('createPublisher over rabbitmq', () => {
  beforeEach(() => {
    amqpSpies.reset()
  })

  it('reuses the channel while the connection is still alive', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(1)
    expect(amqpSpies.connections[0].channel.sent).toHaveLength(2)
  })

  // THE bug: `connectRabbit` was wrapped in `memoizeAsync`, which only drops
  // the memo when the promise REJECTS. Once resolved, the channel stayed
  // cached forever, and amqplib —unlike aio_pika.connect_robust in Python
  // and the per-publish redial in Go— does not repair itself: after
  // restarting the broker, every publish failed on a dead channel forever,
  // until the container was restarted. 8 of the 9 RabbitMQ publishers in the
  // system are Node.
  it('connects again after a broker restart', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    expect(amqpSpies.connects).toBe(1)

    // The broker restarts: amqplib emits 'close' on the connection.
    amqpSpies.connections[0].emitClose()

    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(2)
    expect(amqpSpies.connections[1].channel.sent).toHaveLength(1)
  })

  it('connects again too if what closes is the channel, not the connection', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    amqpSpies.connections[0].channel.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })
    expect(amqpSpies.connects).toBe(2)
  })

  it('does not cache the connection failure: the next publish retries', async () => {
    amqpSpies.fail = true
    const publisher = createPublisher('food-service')
    await expect(publisher.publish('rabbitmq:food.deliver', {})).rejects.toThrow()

    amqpSpies.fail = false
    await expect(publisher.publish('rabbitmq:food.deliver', {})).resolves.toBeUndefined()
    expect(amqpSpies.connects).toBe(2)
  })

  it('an old close does not tear down the connection that replaced it', async () => {
    const publisher = createPublisher('food-service')
    await publisher.publish('rabbitmq:food.deliver', { a: 1 })
    const oldConn = amqpSpies.connections[0]
    oldConn.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 2 })

    // The stray 'close' from the old connection must not invalidate the new
    // one.
    oldConn.emitClose()
    await publisher.publish('rabbitmq:food.deliver', { a: 3 })
    expect(amqpSpies.connects).toBe(2)
  })
})

describe('createRecorder over mysql', () => {
  beforeEach(() => {
    mysqlSpies.reset()
  })

  // The pool was built INSIDE the memo, so every failed CREATE TABLE attempt
  // —the normal case when compose starts MySQL and the service at the same
  // time— left an abandoned pool without closing it. Go closes its connection
  // on failure and Python closes the asyncpg pool; Node with MySQL was the
  // only one that leaked it.
  it('does not create a pool per retry when the CREATE TABLE fails', async () => {
    mysqlSpies.createTableFailures = 2
    const record = createRecorder('food-delivery-service', 'mysql://tt:tt@mysql:3306/food-delivery')
    const payload = Buffer.from('{}')

    await expect(record('rabbitmq:food.deliver', payload)).rejects.toThrow()
    await expect(record('rabbitmq:food.deliver', payload)).rejects.toThrow()
    await expect(record('rabbitmq:food.deliver', payload)).resolves.toBeUndefined()

    expect(mysqlSpies.poolsCreated).toBe(1)
    expect(mysqlSpies.inserts).toBe(1)
  })

  it('memoizes the CREATE TABLE: it does not repeat it on every message', async () => {
    const record = createRecorder('food-delivery-service', 'mysql://tt:tt@mysql:3306/food-delivery')
    for (let i = 0; i < 3; i++) await record('rabbitmq:food.deliver', Buffer.from('{}'))
    expect(mysqlSpies.poolsCreated).toBe(1)
    expect(mysqlSpies.inserts).toBe(3)
  })
})

describe('the consumer retry wait is cancellable', () => {
  beforeEach(() => {
    kafkaSpies.reset()
  })

  // The comment on `delay` promised "or less if `stopped()` becomes true
  // while it waits" and it was a lie: it received no stop signal. With the
  // services shutting down gracefully that shows, because `close()` waits
  // for the loops and every sleeping channel added up to RETRY_DELAY_MS to
  // the shutdown of the service.
  it('close() wakes the wait up instead of sitting out RETRY_DELAY_MS', async () => {
    kafkaSpies.connectBehaviour = () => Promise.reject(new Error('broker not ready'))
    const consumer = createConsumer('test-service', ['kafka:some-topic'], async () => {})
    await consumer.start()

    // Room for the first attempt to fail and for the loop to go to sleep.
    await new Promise((r) => setTimeout(r, 200))

    const startedAt = Date.now()
    await consumer.close()
    expect(Date.now() - startedAt).toBeLessThan(RETRY_DELAY_MS / 2)
  }, 10_000)
})
