# tt-lib-node

Librería compartida para los servicios Node del sistema de venta de
billetes de tren. Da a cada servicio la misma base común, para que la
lógica de infraestructura no se reescriba 20 veces:

| Módulo (`src/`) | Qué resuelve                                                                            |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `config`         | Lee la configuración del servicio desde el entorno.                                     |
| `client`         | Un cliente HTTP uniforme para las llamadas servicio-a-servicio.                          |
| `events`         | Publicar y consumir mensajes con una única API, sea el transporte Kafka o RabbitMQ — el canal decide por su prefijo (`kafka:...` / `rabbitmq:...`), no el servicio. |
| `health`         | Registra `GET /health`, común a todos los servicios Node (Fastify).                     |

Misma forma que sus hermanas [`tt-lib-go`](https://github.com/lucas-test-repos/tt-lib-go)
y [`tt-lib-py`](https://github.com/lucas-test-repos/tt-lib-py) — un
publicador con `publish`/`close`, un consumidor con `start`/`close`, y un
fabricante del manejador de salud — para que un desarrollador que conozca
una la reconozca en las otras dos.

## Por qué es pública

Los 69 servicios Go, Node y Python del sistema —los 70 del árbol generado
menos el frontend, que no consume ninguna librería— dependen de estas tres
librerías por su tag de versión (`v0.1.0`): 24 en Go, 25 en Node y 20 en
Python. Publicarlas como repositorios **públicos** es lo que permite que el
CI de cada uno de esos 69 servicios resuelva la dependencia sin ninguna
credencial. Es la única asimetría de visibilidad deliberada en todo el
conjunto de repositorios.

## Uso

```ts
import { loadConfig, createClient, registerHealth } from '@lucas-test-repos/tt-lib-node'
```

## Desarrollo

```bash
npm ci
npm test
```
