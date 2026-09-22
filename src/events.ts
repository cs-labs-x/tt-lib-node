/**
 * Gives the Node services one single way to publish and consume messages,
 * whatever the transport is. A channel is identified by its prefix —
 * `kafka:order-events` or `rabbitmq:sms.send` — and this library is the one
 * that decides where it goes; neither the sender nor the service knows it.
 * Same shape as `tt-lib-go/events`: a publisher with `publish`/`close`, a
 * consumer with `start`/`close`, and a factory for the handler that records.
 */
import { Kafka, type Producer, type Consumer as KafkaConsumer } from 'kafkajs'
import amqp from 'amqplib'
import { Pool as PgPool } from 'pg'
import mysql from 'mysql2/promise'

export interface Channel {
  transport: string
  name: string
}

/**
 * Splits the transport off the channel name. Returns an empty transport if
 * the channel carries no prefix, which is a declaration error in system.yaml
 * and not something that should be guessed here.
 */
export function splitChannel(channel: string): Channel {
  const i = channel.indexOf(':')
  if (i === -1) return { transport: '', name: channel }
  return { transport: channel.slice(0, i), name: channel.slice(i + 1) }
}

/**
 * Handles a received message. The channel arrives with its prefix, so that
 * the handler knows where it came from without the consumer having to tell
 * it separately.
 */
export type EventHandler = (channel: string, payload: Buffer) => Promise<void>

function kafkaBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? 'kafka:9092').split(',')
}

function rabbitUrl(): string {
  return process.env.RABBITMQ_URL ?? 'amqp://tt:tt@rabbitmq:5672'
}

/**
 * Memoizes an async operation without caching the failure.
 *
 * The `publisher.ts` that the generator emits today for order-service was
 * already solving this for its kafka connection: it memoizes the connection
 * PROMISE, not the result, so that two concurrent cold publishes wait on
 * the same one instead of each firing its own negotiation with the
 * broker. And it does not cache the failure — if the broker is not up yet
 * (docker compose starts the services without waiting for kafka or rabbitmq
 * to accept connections), the next call must be able to retry instead of
 * being stuck with the error forever. Here that same pattern is generalised
 * to the publisher's two transports and to the recorder's table
 * preparation, instead of repeating it three times.
 *
 * Exported because it is pure logic and it is the piece that avoids the
 * failure that did happen in the first version of `tt-lib-go`: memoizing
 * connection and error together leaves the publisher/recorder unusable
 * forever after a transient failure of the broker or the database. Its
 * contract is verified directly in `tests/events.test.ts`, without
 * depending on a real broker or a real database.
 */
export function memoizeAsync<T>(op: () => Promise<T>): () => Promise<T> {
  let inFlight: Promise<T> | undefined
  return () => {
    inFlight ??= op().catch((err: unknown) => {
      inFlight = undefined
      throw err
    })
    return inFlight
  }
}

export interface Publisher {
  /** Publishes the payload on the given channel, serialized as JSON. */
  publish(channel: string, payload: unknown): Promise<void>
  /** Closes the open connections. */
  close(): Promise<void>
}

type AmqpConnection = Awaited<ReturnType<typeof amqp.connect>>
type AmqpChannel = Awaited<ReturnType<AmqpConnection['createChannel']>>

/**
 * Creates the service publisher. It does not connect yet: the connection to
 * each transport is opened on the first publish that uses it, so that the
 * start-up of the service is not blocked if the broker is not ready yet.
 */
export function createPublisher(serviceName: string): Publisher {
  const kafka = new Kafka({ clientId: serviceName, brokers: kafkaBrokers() })
  const producer: Producer = kafka.producer()
  // kafkajs DOES repair itself: its producer reconnects on its own after
  // losing the broker, so here memoizing the initial `connect()` is enough.
  const connectKafka = memoizeAsync(() => producer.connect())

  // amqplib does NOT repair itself, and that is why this transport cannot
  // use plain `memoizeAsync`. `memoizeAsync` only drops the memo when the
  // promise REJECTS; once it resolves, the channel stays cached forever.
  // After restarting RabbitMQ, every publish failed on a dead channel
  // forever and only restarting the container recovered it — the twin, on
  // the publisher side, of the Node consumer crash bug that was already
  // fixed (`supervise`), and it hits 8 of the 9 RabbitMQ publishers in the
  // system. Python does not have it because it uses
  // `aio_pika.connect_robust`, and Go does not either because it redials on
  // every publish.
  //
  // The fix: DROP the memo as soon as the connection or the channel closes
  // or errors, so that the next publish dials again. The `generation` tells
  // "this session" apart from the next one: the listeners of a connection
  // that was already replaced fire on an old generation and do not tear
  // down the new connection another publish has just opened.
  let rabbitConn: AmqpConnection | undefined
  let rabbitChannel: Promise<AmqpChannel> | undefined
  let rabbitGeneration = 0

  function dropRabbit(generation: number): void {
    if (generation !== rabbitGeneration) return // this session was replaced
    rabbitGeneration++
    rabbitChannel = undefined
    rabbitConn = undefined
  }

  async function openRabbit(generation: number): Promise<AmqpChannel> {
    const conn = await amqp.connect(rabbitUrl())
    // 'error' needs a listener no matter what: amqplib emits it on an
    // EventEmitter, and an unheard 'error' brings the whole process down.
    conn.on('close', () => {
      dropRabbit(generation)
    })
    conn.on('error', () => {
      dropRabbit(generation)
    })
    let ch: AmqpChannel
    try {
      ch = await conn.createChannel()
    } catch (err: unknown) {
      await conn.close().catch(() => {})
      throw err
    }
    ch.on('close', () => {
      dropRabbit(generation)
    })
    ch.on('error', () => {
      dropRabbit(generation)
    })
    rabbitConn = conn
    return ch
  }

  function connectRabbit(): Promise<AmqpChannel> {
    if (rabbitChannel !== undefined) return rabbitChannel
    const generation = rabbitGeneration
    // The failure is not cached either, same as in `memoizeAsync`.
    rabbitChannel = openRabbit(generation).catch((err: unknown) => {
      dropRabbit(generation)
      throw err
    })
    return rabbitChannel
  }

  async function publish(channel: string, payload: unknown): Promise<void> {
    const { transport, name } = splitChannel(channel)
    const body = Buffer.from(JSON.stringify(payload))

    switch (transport) {
      case 'kafka':
        await connectKafka()
        await producer.send({ topic: name, messages: [{ value: body }] })
        return
      case 'rabbitmq': {
        const ch = await connectRabbit()
        await ch.assertQueue(name, { durable: true })
        ch.sendToQueue(name, body)
        return
      }
      default:
        throw new Error(`channel "${channel}" has no recognized transport`)
    }
  }

  async function close(): Promise<void> {
    await producer.disconnect().catch(() => {})
    await rabbitConn?.close().catch(() => {})
  }

  return { publish, close }
}

export interface Consumer {
  /**
   * Starts one loop per channel and returns without waiting for them to
   * connect. These consumers live inside long-lived Fastify services: if
   * `start` blocked until every broker accepted the connection, a broker
   * that is down would delay the start-up of the whole service — and with
   * up to 18 consumers in the Node share, the start-up of the whole stack.
   * Each channel retries on its own while the rest of the service is
   * already serving requests.
   */
  start(): Promise<void>
  /** Stops the loops gracefully and waits for them to finish. */
  close(): Promise<void>
}

/**
 * Gap between two attempts to start a channel loop when the broker is not
 * ready or the session drops. Same value and same criterion as
 * `rabbitRetryDelay` in tt-lib-go: fixed, with no growing backoff, which is
 * just enough not to leave a channel dead without hammering the broker.
 */
export const RETRY_DELAY_MS = 2000

/** Creates the service consumer for the given channels. */
export function createConsumer(serviceName: string, channels: string[], handler: EventHandler): Consumer {
  const kafka = new Kafka({ clientId: serviceName, brokers: kafkaBrokers() })
  const kafkaConsumers = new Set<KafkaConsumer>()
  const rabbitConns = new Set<AmqpConnection>()
  const rabbitChannels = new Set<AmqpChannel>()
  // `close()` raises it: it is the only way to tell "the session dropped,
  // retry" apart from "we are being stopped, do not retry".
  let stopped = false
  const loops: Promise<void>[] = []

  // Retry waits in flight. The comment on `delay` said "or less if
  // `stopped()` becomes true while it waits" and it was false: `delay(ms)`
  // received no stop signal and never resolved early. Of the two ways out
  // —fix the comment, or make the wait really cancellable— the second one is
  // chosen, because since the services shut down gracefully the difference
  // shows: `close()` waits for the loops, and a channel stopped halfway
  // through its 2s wait delayed the shutdown of the whole service, once for
  // every channel that was retrying. The equivalent already exists in the
  // other two libraries (`sleepOrDone` in tt-lib-go and the asyncio task
  // cancellation in tt-lib-py); Node was the only one that slept without
  // being able to wake up.
  const pendingWaits = new Set<() => void>()

  /** Waits `ms`, or less if `close()` wakes the wait up while it sleeps. */
  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        clearTimeout(timer)
        pendingWaits.delete(wake)
        resolve()
      }
      const timer = setTimeout(wake, ms)
      // A pending timer must not stop the process from exiting.
      ;(timer as unknown as { unref?: () => void }).unref?.()
      pendingWaits.add(wake)
    })
  }

  /**
   * Runs a channel session over and over until `close()` stops the
   * consumer. `runSession` must resolve ONLY when the session ends
   * (connection lost, runner down), not when it starts.
   *
   * This loop is the fix for a real failure, not a precaution: without it,
   * `startKafkaChannel` started the kafkajs runner and never looked at it
   * again. When the broker returned GroupCoordinatorNotAvailable —a single
   * broker with `offsets.topic.replication.factor=3`, see infra.yml—,
   * kafkajs logged "[Consumer] Crash ... [Consumer] Stopped" and stopped
   * there: the service stayed healthy and serving /health, but it NEVER
   * consumed another message, not even after the broker was fixed. Only
   * restarting the container recovered it. The Go and the Python consumers
   * did retry (their loops filled the log with retries), so Node was the
   * only one of the three languages that gave up — and with 18 consumers
   * Node was, on top of that, the largest share.
   */
  function supervise(channel: string, runSession: () => Promise<void>): Promise<void> {
    return (async () => {
      while (!stopped) {
        try {
          await runSession()
        } catch (err: unknown) {
          if (stopped) return
          console.error(`${serviceName}: ${channel}: ${String(err)}`)
        }
        if (stopped) return
        await delay(RETRY_DELAY_MS)
      }
    })()
  }

  async function runKafkaSession(channel: string, topic: string): Promise<void> {
    const consumer = kafka.consumer({ groupId: serviceName })
    kafkaConsumers.add(consumer)
    try {
      // Subscribes to CRASH before connecting: kafkajs runs its runner in
      // the background, so `run()` resolves as soon as it starts and
      // without this promise the session would "end" right away and the
      // loop above would restart it for nothing every RETRY_DELAY_MS. With
      // it, the session lasts as long as the runner does: it resolves when
      // kafkajs brings it down, which is exactly when it is time to
      // reconnect.
      const crashed = new Promise<void>((resolve) => {
        consumer.on(consumer.events.CRASH, () => {
          resolve()
        })
        consumer.on(consumer.events.STOP, () => {
          resolve()
        })
      })
      await consumer.connect()
      await consumer.subscribe({ topic, fromBeginning: false })
      await consumer.run({
        eachMessage: async ({ message }) => {
          await handler(channel, message.value ?? Buffer.alloc(0))
        },
      })
      await crashed
    } finally {
      kafkaConsumers.delete(consumer)
      await consumer.disconnect().catch(() => {})
    }
  }

  async function runRabbitSession(channel: string, queue: string): Promise<void> {
    // One connection per session, not one shared by the whole consumer: the
    // shared one could not be replaced after a drop without dragging the
    // other channels down, and that coupling is what prevented retrying.
    const conn = await amqp.connect(rabbitUrl())
    rabbitConns.add(conn)
    const ch = await conn.createChannel()
    rabbitChannels.add(ch)
    try {
      await ch.assertQueue(queue, { durable: true })
      // Resolves when the connection or the channel closes: same as in
      // Kafka, `consume` only registers the callback and returns right away.
      const closed = new Promise<void>((resolve) => {
        conn.on('close', () => {
          resolve()
        })
        conn.on('error', () => {
          resolve()
        })
        ch.on('close', () => {
          resolve()
        })
      })
      await ch.consume(queue, (msg) => {
        if (!msg) return // the channel closed (graceful stop)
        handler(channel, msg.content)
          .then(() => {
            ch.ack(msg)
          })
          .catch((err: unknown) => {
            console.error(`${serviceName}: processing ${channel}: ${String(err)}`)
            ch.nack(msg, false, true)
          })
      })
      await closed
    } finally {
      rabbitChannels.delete(ch)
      rabbitConns.delete(conn)
      await ch.close().catch(() => {})
      await conn.close().catch(() => {})
    }
  }

  async function start(): Promise<void> {
    for (const channel of channels) {
      const { transport, name } = splitChannel(channel)
      if (transport === 'kafka') {
        loops.push(supervise(channel, () => runKafkaSession(channel, name)))
      } else if (transport === 'rabbitmq') {
        loops.push(supervise(channel, () => runRabbitSession(channel, name)))
      } else {
        // A channel misspelled in system.yaml is not retried: retrying is
        // not going to fix it. It is logged and that channel stays out.
        console.error(`${serviceName}: channel "${channel}" has no recognized transport`)
      }
    }
    // `start` does NOT wait for the loops: it returns right away so that
    // the service serves requests while the channels connect.
  }

  async function close(): Promise<void> {
    stopped = true
    // Wakes the retry waits up: without this, `close()` would hang up to
    // RETRY_DELAY_MS for every channel that was sleeping.
    for (const wake of [...pendingWaits]) wake()
    // Closing whatever is alive makes the session in flight resolve and the
    // loop exit through the `if (stopped) return`.
    await Promise.all([...kafkaConsumers].map((c) => c.disconnect().catch(() => {})))
    await Promise.all([...rabbitChannels].map((c) => c.close().catch(() => {})))
    await Promise.all([...rabbitConns].map((c) => c.close().catch(() => {})))
    await Promise.all(loops)
  }

  return { start, close }
}

const CREATE_TABLE_POSTGRES = `CREATE TABLE IF NOT EXISTS received_events (
  id SERIAL PRIMARY KEY,
  channel VARCHAR(255) NOT NULL,
  payload TEXT NOT NULL,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

const CREATE_TABLE_MYSQL = `CREATE TABLE IF NOT EXISTS received_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  channel VARCHAR(255) NOT NULL,
  payload TEXT NOT NULL,
  received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

type SqlEngine = 'postgres' | 'mysql'

/**
 * Works out the SQL engine from the connection string. Returns undefined for
 * anything that is not supported SQL (today: MongoDB, in
 * catalog-sync-service) or if there is no database.
 */
function engineOf(databaseUrl: string | undefined): SqlEngine | undefined {
  if (!databaseUrl) return undefined
  if (databaseUrl.startsWith('postgresql://') || databaseUrl.startsWith('postgres://')) return 'postgres'
  if (databaseUrl.startsWith('mysql://')) return 'mysql'
  return undefined
}

function logRecorder(serviceName: string): EventHandler {
  return async (channel, payload) => {
    console.log(`${serviceName}: received from ${channel}: ${payload.toString()}`)
  }
}

/**
 * Returns the handler that records what is received. If the service has a
 * supported SQL database (PostgreSQL via `pg`, MySQL via `mysql2`), it
 * writes a row into `received_events`; if it has no database, or the engine
 * is not supported SQL, it writes to the log. It is a decision, not an
 * oversight: covering the three engines in the three languages would be
 * nine code paths for nine consumers.
 *
 * The placeholders and the auto-increment type are not portable between
 * PostgreSQL and MySQL, so each engine has its own create and insert
 * statement.
 */
export function createRecorder(serviceName: string, databaseUrl?: string): EventHandler {
  const engine = engineOf(databaseUrl)
  if (!engine) return logRecorder(serviceName)

  if (engine === 'postgres') {
    const pool = new PgPool({ connectionString: databaseUrl })
    const prepareOnce = memoizeAsync(() => pool.query(CREATE_TABLE_POSTGRES))

    return async (channel, payload) => {
      await prepareOnce()
      await pool.query('INSERT INTO received_events (channel, payload) VALUES ($1, $2)', [
        channel,
        payload.toString(),
      ])
    }
  }

  // mysql2 accepts the connection URL (`mysql://user:pass@host:port/db`)
  // directly in `createPool`, so unlike the Go driver there is no need to
  // convert it into a DSN of its own.
  //
  // The pool is created OUTSIDE the memo, exactly like the PostgreSQL one up
  // here, and only the CREATE TABLE is memoized. Before, the pool was built
  // INSIDE: every failed attempt —the normal case when compose starts
  // MySQL and the service at the same time— left an abandoned pool without
  // closing it, one per message received until the database answered. Go
  // closes its connection on failure and Python closes the asyncpg pool;
  // Node with MySQL was the only one that leaked it. Creating the pool
  // outside is simpler than closing it in the `catch` and on top of that it
  // creates none extra: `mysql.createPool` is lazy, it opens no sockets
  // until the first query.
  const pool = mysql.createPool(databaseUrl as string)
  const prepareOnce = memoizeAsync(() => pool.query(CREATE_TABLE_MYSQL))

  return async (channel, payload) => {
    await prepareOnce()
    await pool.query('INSERT INTO received_events (channel, payload) VALUES (?, ?)', [channel, payload.toString()])
  }
}
