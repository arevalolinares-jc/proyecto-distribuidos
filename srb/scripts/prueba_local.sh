#!/usr/bin/env bash
# Prueba rápida en UNA máquina: levanta los 4 servicios, corre el generador y apaga todo.
# Uso: scripts/prueba_local.sh [duracion_s] [clientes] [solo_consulta] [extra args del generador...]
# Variable opcional: DB="dbname=srb"
set -u
cd "$(dirname "$0")/.."
DUR=${1:-20}
CLI=${2:-3}
SOLO=${3:-2}
shift 3 2>/dev/null || shift $# 2>/dev/null
DB=${DB:-dbname=srb}

mkdir -p logs
PIDS=()
cleanup() { kill -TERM "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

./bin/persistencia --db "$DB" --reset > logs/persistencia.log 2>&1 &
PERS=$!; PIDS+=($PERS)
sleep 1
./bin/rmc          > logs/rmc.log 2>&1 &            PIDS+=($!)
./bin/cancelaciones > logs/cancelaciones.log 2>&1 & PIDS+=($!)
./bin/gestor       > logs/gestor.log 2>&1 &         PIDS+=($!)
sleep 1

./bin/generador --clientes "$CLI" --solo-consulta "$SOLO" --duracion "$DUR" \
                --calentamiento 2 --csv logs/resultados.csv "$@"

kill -USR1 "$PERS"; sleep 0.5
cleanup
trap - EXIT
echo
echo "== Estado final de la BD (ultimas lineas) =="; tail -n 3 logs/persistencia.log
echo "== Resumen del GT =="; grep GT-STATS logs/gestor.log | tail -n 4
