export { loadConfig, type ServiceConfig } from './config'
export { createClient, type ServiceClient } from './client'
export { registerHealth } from './health'
export {
  splitChannel,
  createPublisher,
  createConsumer,
  createRecorder,
  type Channel,
  type EventHandler,
  type Publisher,
  type Consumer,
} from './events'
