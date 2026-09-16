/**
 * Da a los servicios Node una forma única de publicar y consumir mensajes,
 * sea cual sea el transporte. Un canal se identifica por su prefijo —
 * `kafka:order-events` o `rabbitmq:sms.send` — y es esta librería la que
 * decide por dónde va; ni el emisor ni el servicio lo saben. Misma forma que
 * `tt-lib-go/events`: un publicador con `publish`/`close`, un consumidor con
 * `start`/`close`, y un fabricante del manejador que registra.
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
 * Separa el transporte del nombre del canal. Devuelve transporte vacío si
 * el canal no lleva prefijo, que es un error de declaración del servicio
 * y no algo que deba adivinarse aquí.
 */
export function splitChannel(channel: string): Channel {
  const i = channel.indexOf(':')
  if (i === -1) return { transport: '', name: channel }
  return { transport: channel.slice(0, i), name: channel.slice(i + 1) }
}

/**
 * Procesa un mensaje recibido. El canal llega con su prefijo, para que el
 * manejador sepa de dónde vino sin que el consumidor tenga que contarlo
 * aparte.
 */
export type EventHandler = (channel: string, payload: Buffer) => Promise<void>

function kafkaBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? 'kafka:9092').split(',')
}

function rabbitUrl(): string {
  return process.env.RABBITMQ_URL ?? 'amqp://tt:tt@rabbitmq:5672'
}

/**
 * Memoiza una operación asíncrona sin cachear el fallo.
 *
 * El `publisher.ts` que hoy emite el generador para order-service ya
 * resolvía esto para su conexión a kafka: memoiza la PROMESA de conexión,
 * no el resultado, para que dos publicaciones concurrentes en frío esperen
 * a la misma en vez de disparar cada una su propia negociación con el
 * broker. Y no cachea el fallo — si el broker todavía no está arriba
 * (docker compose arranca los servicios sin esperar a que kafka o rabbitmq
 * acepten conexiones), la siguiente llamada debe poder reintentar en vez de
 * quedarse con el error para siempre. Aquí se generaliza ese mismo patrón a
 * los dos transportes del publicador y a la preparación de tabla del
 * recorder, en vez de repetirlo tres veces.
 *
 * Exportado porque es lógica pura y es el componente que evita el fallo que
 * sí ocurrió en la versión inicial de `tt-lib-go`: memoizar conexión y
 * error juntos deja el publicador/recorder inutilizado para siempre tras un
 * fallo transitorio del broker o la base de datos. Su contrato se
 * verifica directamente en `tests/events.test.ts`, sin depender de un
 * broker o una base de datos real.
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
  /** Publica el payload en el canal indicado, serializado como JSON. */
  publish(channel: string, payload: unknown): Promise<void>
  /** Cierra las conexiones abiertas. */
  close(): Promise<void>
}

type AmqpConnection = Awaited<ReturnType<typeof amqp.connect>>
type AmqpChannel = Awaited<ReturnType<AmqpConnection['createChannel']>>

/**
 * Crea el publicador del servicio. No conecta todavía: la conexión a cada
 * transporte se abre en la primera publicación que lo use, para no
 * bloquear el arranque del servicio si el broker aún no está listo.
 */
export function createPublisher(serviceName: string): Publisher {
  const kafka = new Kafka({ clientId: serviceName, brokers: kafkaBrokers() })
  const producer: Producer = kafka.producer()
  // kafkajs SÍ se autorrepara: su productor reconecta solo tras perder el
  // broker, así que aquí basta con memoizar el `connect()` inicial.
  const connectKafka = memoizeAsync(() => producer.connect())

  // amqplib NO se autorrepara, y por eso este transporte no puede usar
  // `memoizeAsync` a secas. `memoizeAsync` solo suelta el memo cuando la
  // promesa RECHAZA; una vez resuelta, el canal queda cacheado para siempre.
  // Tras reiniciar RabbitMQ, cada publicación fallaba sobre un canal muerto
  // indefinidamente y solo lo recuperaba reiniciar el contenedor — el gemelo,
  // en el lado publicador, del bug de crash del consumidor Node que ya se
  // corrigió (`supervise`), y que afecta a 8 de los 9 publicadores de
  // RabbitMQ del sistema. Python no lo tiene porque usa
  // `aio_pika.connect_robust`, y Go tampoco porque redial en cada
  // publicación.
  //
  // La corrección: SOLTAR el memo en cuanto la conexión o el canal se cierran
  // o dan error, para que la siguiente publicación vuelva a marcar. La
  // `generation` distingue "esta sesión" de la siguiente: los listeners de
  // una conexión ya reemplazada disparan sobre una generación vieja y no
  // tiran la conexión nueva que otro publicador acaba de abrir.
  let rabbitConn: AmqpConnection | undefined
  let rabbitChannel: Promise<AmqpChannel> | undefined
  let rabbitGeneration = 0

  function dropRabbit(generation: number): void {
    if (generation !== rabbitGeneration) return // esta sesión ya se reemplazó
    rabbitGeneration++
    rabbitChannel = undefined
    rabbitConn = undefined
  }

  async function openRabbit(generation: number): Promise<AmqpChannel> {
    const conn = await amqp.connect(rabbitUrl())
    // 'error' necesita listener sí o sí: amqplib lo emite sobre un
    // EventEmitter, y un 'error' sin escuchar tumba el proceso entero.
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
    // El fallo tampoco se cachea, igual que en `memoizeAsync`.
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
        throw new Error(`canal "${channel}" sin transporte reconocido`)
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
   * Arranca un bucle por canal y devuelve sin esperar a que conecten. Estos
   * consumidores viven dentro de servicios Fastify de larga vida: si
   * `start` bloqueara hasta que cada broker aceptase la conexión, un broker
   * caído retrasaría el arranque del servicio entero — y con hasta 18
   * consumidores en el reparto Node, el arranque del stack completo. Cada
   * canal reintenta por su cuenta mientras el resto del servicio ya atiende
   * peticiones.
   */
  start(): Promise<void>
  /** Para los bucles de forma ordenada y espera a que terminen. */
  close(): Promise<void>
}

/**
 * Separación entre dos intentos de arrancar el bucle de un canal cuando el
 * broker no está listo o la sesión se cae. Mismo valor y mismo criterio que
 * `rabbitRetryDelay` en tt-lib-go: fijo, sin backoff creciente, que es lo
 * justo para no dejar un canal muerto sin martillear al broker.
 */
export const RETRY_DELAY_MS = 2000

/** Crea el consumidor del servicio para los canales indicados. */
export function createConsumer(serviceName: string, channels: string[], handler: EventHandler): Consumer {
  const kafka = new Kafka({ clientId: serviceName, brokers: kafkaBrokers() })
  const kafkaConsumers = new Set<KafkaConsumer>()
  const rabbitConns = new Set<AmqpConnection>()
  const rabbitChannels = new Set<AmqpChannel>()
  // Lo levanta `close()`: es la única forma de distinguir "la sesión se cayó,
  // reintenta" de "nos están parando, no reintentes".
  let stopped = false
  const loops: Promise<void>[] = []

  // Esperas de reintento en curso. El comentario de `delay` decía "o menos si
  // `stopped()` pasa a true mientras espera" y era falso: `delay(ms)` no
  // recibía ninguna señal de parada y nunca resolvía antes de tiempo. De las
  // dos salidas —corregir el comentario o hacer la espera cancelable de
  // verdad— se elige la segunda, porque desde que los servicios cierran de
  // forma ordenada la diferencia se nota: `close()` espera a los bucles, y un
  // canal parado en mitad de su espera de 2s retrasaba el cierre del servicio
  // entero por cada canal que estuviera reintentando. El equivalente ya
  // existe en las otras dos librerías (`sleepOrDone` en tt-lib-go y la
  // cancelación de tareas de asyncio en tt-lib-py); Node era el único que
  // dormía sin poder despertarse.
  const pendingWaits = new Set<() => void>()

  /** Espera `ms`, o menos si `close()` despierta la espera mientras duerme. */
  function delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        clearTimeout(timer)
        pendingWaits.delete(wake)
        resolve()
      }
      const timer = setTimeout(wake, ms)
      // Un timer pendiente no debe impedir que el proceso termine.
      ;(timer as unknown as { unref?: () => void }).unref?.()
      pendingWaits.add(wake)
    })
  }

  /**
   * Corre una sesión de canal una y otra vez hasta que `close()` para el
   * consumidor. `runSession` debe resolver SÓLO cuando la sesión termina
   * (conexión perdida, runner caído), no cuando arranca.
   *
   * Este bucle es la corrección de un fallo real, no una precaución: sin él,
   * `startKafkaChannel` arrancaba el runner de kafkajs y no volvía a mirarlo.
   * Cuando el broker devolvió GroupCoordinatorNotAvailable —un solo broker
   * con `offsets.topic.replication.factor=3`, ver infra.yml—, kafkajs
   * registró "[Consumer] Crash ... [Consumer] Stopped" y ahí se quedó: el
   * servicio seguía sano y sirviendo /health, pero no volvió a consumir un
   * mensaje NUNCA, ni después de arreglar el broker. Sólo lo recuperaba
   * reiniciar el contenedor. Los consumidores de Go y de Python sí
   * reintentaban (sus bucles llenaban el log de reintentos), así que Node
   * era el único de los tres lenguajes que se rendía — y con 18 consumidores
   * Node era, además, el reparto mayoritario.
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
      // Se suscribe al CRASH antes de conectar: kafkajs corre su runner en
      // segundo plano, así que `run()` resuelve en cuanto arranca y sin esta
      // promesa la sesión "terminaría" de inmediato y el bucle de arriba la
      // reiniciaría en vacío cada RETRY_DELAY_MS. Con ella, la sesión dura
      // lo que dure el runner: se resuelve cuando kafkajs lo tumba, que es
      // justo cuando toca reconectar.
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
    // Una conexión por sesión, no una compartida por todo el consumidor: la
    // compartida no se podía reemplazar tras una caída sin arrastrar a los
    // demás canales, y ese acoplamiento es lo que impedía reintentar.
    const conn = await amqp.connect(rabbitUrl())
    rabbitConns.add(conn)
    const ch = await conn.createChannel()
    rabbitChannels.add(ch)
    try {
      await ch.assertQueue(queue, { durable: true })
      // Se resuelve cuando la conexión o el canal se cierran: igual que en
      // Kafka, `consume` sólo registra el callback y vuelve enseguida.
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
        if (!msg) return // el canal se cerró (parada ordenada)
        handler(channel, msg.content)
          .then(() => {
            ch.ack(msg)
          })
          .catch((err: unknown) => {
            console.error(`${serviceName}: procesando ${channel}: ${String(err)}`)
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
        // Un canal mal escrito por el servicio no se reintenta: reintentar no
        // lo va a arreglar. Se registra y ese canal se queda fuera.
        console.error(`${serviceName}: canal "${channel}" sin transporte reconocido`)
      }
    }
    // `start` NO espera a los bucles: devuelve enseguida para que el
    // servicio atienda peticiones mientras los canales conectan.
  }

  async function close(): Promise<void> {
    stopped = true
    // Despierta las esperas de reintento: sin esto, `close()` se quedaría
    // hasta RETRY_DELAY_MS por cada canal que estuviera durmiendo.
    for (const wake of [...pendingWaits]) wake()
    // Cerrar lo que esté vivo hace que la sesión en curso resuelva y el
    // bucle salga por el `if (stopped) return`.
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
 * Deduce el motor SQL a partir de la cadena de conexión. Devuelve undefined
 * para cualquier cosa que no sea SQL soportado (hoy: MongoDB, en
 * catalog-sync-service) o si no hay base de datos.
 */
function engineOf(databaseUrl: string | undefined): SqlEngine | undefined {
  if (!databaseUrl) return undefined
  if (databaseUrl.startsWith('postgresql://') || databaseUrl.startsWith('postgres://')) return 'postgres'
  if (databaseUrl.startsWith('mysql://')) return 'mysql'
  return undefined
}

function logRecorder(serviceName: string): EventHandler {
  return async (channel, payload) => {
    console.log(`${serviceName}: recibido de ${channel}: ${payload.toString()}`)
  }
}

/**
 * Devuelve el manejador que registra lo recibido. Si el servicio tiene una
 * base de datos SQL soportada (PostgreSQL vía `pg`, MySQL vía `mysql2`),
 * escribe una fila en `received_events`; si no tiene base de datos, o el
 * motor no es SQL soportado, registra en el log. Es una decisión, no un
 * olvido: cubrir los tres motores en los tres lenguajes serían nueve
 * caminos de código para nueve consumidores.
 *
 * Los placeholders y el tipo autoincremental no son portables entre
 * PostgreSQL y MySQL, así que cada motor tiene su propia sentencia de
 * creación e inserción.
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

  // mysql2 acepta la URL de conexión (`mysql://user:pass@host:puerto/bd`)
  // directamente en `createPool`, así que a diferencia del driver de Go no
  // hace falta convertirla a un DSN propio.
  //
  // El pool se crea FUERA del memo, exactamente igual que el de PostgreSQL de
  // aquí arriba, y solo se memoiza el CREATE TABLE. Antes el pool se
  // construía DENTRO: cada intento fallido —el caso normal cuando compose
  // arranca MySQL y el servicio a la vez— dejaba un pool abandonado sin
  // cerrar, uno por mensaje recibido hasta que la base de datos respondiera.
  // Go cierra su conexión al fallar y Python cierra el pool de asyncpg; Node
  // con MySQL era el único que la fugaba. Crear el pool fuera es más simple
  // que cerrarlo en el `catch` y además no crea ninguno de más:
  // `mysql.createPool` es perezoso, no abre sockets hasta la primera consulta.
  const pool = mysql.createPool(databaseUrl as string)
  const prepareOnce = memoizeAsync(() => pool.query(CREATE_TABLE_MYSQL))

  return async (channel, payload) => {
    await prepareOnce()
    await pool.query('INSERT INTO received_events (channel, payload) VALUES (?, ?)', [channel, payload.toString()])
  }
}
