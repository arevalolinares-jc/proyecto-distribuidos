# SRB – Sistema de Reserva de Boletos (arquitectura base, primera entrega)

Implementado: Cliente/Generador de carga, Gestor de Transacciones (GT), RMC,
Servicio de Cancelaciones y Servicio de Persistencia (PostgreSQL), todo por ZeroMQ.
Operaciones activas: **consulta** y **reserva** (síncronas) y **cancelación** (asíncrona,
con notificación). La **modificación** responde ERROR (queda para la segunda entrega).
No hay tolerancia a fallas en esta entrega.

## Estructura

```
srb/
├── Makefile
├── README.md
├── scripts/prueba_local.sh        prueba rápida de todo en una sola máquina
└── src/
    ├── common.hpp                 Mensaje, Evento, Reserva, serialización, utilidades ZeroMQ
    ├── servicio_persistencia.cpp  ServicioPersistencia   (REP  + PostgreSQL)
    ├── servicio_rmc.cpp           ServicioRMC            (REP  <- GT, REQ -> Persistencia)
    ├── servicio_cancelaciones.cpp ServicioCancelaciones  (PULL <- GT, REQ -> Persistencia, PUB)
    ├── gestor_transacciones.cpp   GestorTransacciones    (ROUTER, DEALER, PUSH)
    └── generador_carga.cpp        GeneradorCarga, Cliente (REQ, SUB), Metricas
```

## Requisitos (Ubuntu/Debian)

```bash
sudo apt install -y build-essential libzmq3-dev libpq-dev libpqxx-dev postgresql
# cppzmq: solo dos headers (zmq.hpp y zmq_addon.hpp) en /usr/local/include o en src/
```

PostgreSQL con autenticación por socket de Unix (sin contraseña). El servicio de
persistencia debe correr con el **mismo usuario de Linux** que tenga rol en PostgreSQL:

```bash
sudo -u postgres createuser $USER
sudo -u postgres createdb -O $USER srb
```

## Compilar

```bash
make            # genera bin/persistencia bin/rmc bin/cancelaciones bin/gestor bin/generador
```

## Prueba rápida en una máquina

```bash
scripts/prueba_local.sh 20 3 2        # 20 s, 3 clientes, 2 de ellos solo consultan
scripts/prueba_local.sh 20 5 3 --rpm 0    # argumentos extra pasan al generador
```

Deja los logs de cada proceso en `logs/` y el CSV de métricas en `logs/resultados.csv`.

## Despliegue en 3 máquinas (una por capa)

Puertos por defecto: GT 5555 (clientes), RMC 5556, Cancelaciones PULL 5557 y PUB 5558,
Persistencia 5559. Abrir en el firewall: 5555 y 5558 en la máquina 2; 5559 en la máquina 3.

| Máquina | Capa | Comandos |
|---|---|---|
| 3 | Almacenamiento | `./bin/persistencia --reset` |
| 2 | Lógica de negocio | `./bin/rmc --persistencia tcp://IP3:5559`<br>`./bin/cancelaciones --persistencia tcp://IP3:5559`<br>`./bin/gestor` |
| 1 | Consumidores | `./bin/generador --gt tcp://IP2:5555 --notif tcp://IP2:5558 --clientes 3 --solo-consulta 2 --duracion 120 --csv resultados.csv` |

Orden de arranque: persistencia, RMC, cancelaciones, GT y al final el generador.

### Parámetros

- **persistencia:** `--bind` `--db "dbname=srb"` `--reset` (borra y recarga) `--mes 202611`
  `--eventos 50` `--dobles 20` `--asientos 100` `--volcar-cada SEG`
- **rmc / cancelaciones:** `--persistencia tcp://host:5559` `--timeout-ms 5000`
- **gestor:** `--bind tcp://*:5555` `--rmc tcp://host:5556` `--cancel tcp://host:5557`
- **generador:** `--clientes N` `--solo-consulta K` `--rpm N` (0 = sin pausa) `--duracion SEG`
  `--pct-consulta 70 --pct-reserva 30 --pct-cancelar 0` (clientes mixtos, suman 100)
  `--calentamiento SEG` (descarta los primeros segundos) `--timeout-ms` `--semilla`
  `--arquitectura base|escalable` `--csv archivo` `--eventos 50 --dobles 20 --max-asientos 4`

Los clientes que **no** son "solo consulta" siguen los porcentajes `--pct-*`.

## Qué mirar en la sustentación

- **Estado de la BD:** `persistencia` imprime la tabla completa al arrancar y al
  apagarse; `kill -USR1 <pid de persistencia>` la imprime cuando se quiera, y
  `--volcar-cada 5` imprime un resumen cada 5 s.
- **Operaciones de cada proceso:** cada servicio escribe una línea por operación
  con su resultado (EXITO / RECHAZO / ERROR).
- **TPM:** el GT imprime `GT-STATS` cada 10 s y al detenerse (Ctrl+C).
- **Tiempos de respuesta:** los mide el generador con el reloj del cliente y los
  agrega al CSV (promedio, desviación estándar, p95 y TPM por tipo).

## Formato de los mensajes

Texto de una línea, 14 campos separados por `|` (ver `common.hpp`). `fecha` es
un entero AAAAMMDD y `mes` un entero AAAAMM. Entre capas de negocio y hacia
Persistencia: `CONSULTA` con `mes > 0` lista los eventos del mes y con `mes == 0`
consulta la disponibilidad de (idEvento, idOcurrencia).
