// servicio_rmc.cpp  (Máquina 2: capa de lógica de negocio)
// Servicio de Reservas, Modificaciones y Consultas. Recibe del Gestor de
// Transacciones (DEALER -> REP) y consulta a Persistencia por REQ/REP.
//
// Uso: ./rmc [--bind tcp://*:5556] [--persistencia tcp://localhost:5559] [--timeout-ms 5000]
#include "common.hpp"

using namespace srb;

class ServicioRMC {
public:
    ServicioRMC(const std::string& bind, const std::string& epPersistencia, int timeoutMs)
        : ctx_(1),
          sockGT(crearSocket(ctx_, zmq::socket_type::rep)),
          persistencia(ctx_, epPersistencia, timeoutMs) {
        sockGT.bind(bind);
    }

    void iniciar() {
        logLinea("RMC", "escuchando; esperando solicitudes del GT");
        std::vector<std::string> tramas;
        while (!g_parar) {
            if (!hayEntrada(sockGT, 500)) continue;
            if (!recibirMultipart(sockGT, tramas)) continue;

            // El GT envía [correlativo][mensaje]; el REP ya quitó el delimitador vacío.
            std::string corr = tramas.size() >= 2 ? tramas.front() : "0";
            Mensaje m, r;
            if (tramas.size() < 2 || !deserializar(tramas.back(), m)) {
                r.resultado = Estado::ERROR;
                logLinea("RMC", "mensaje mal formado -> ERROR");
            } else {
                switch (m.tipo) {
                    case TipoOperacion::CONSULTA: r = atenderConsulta(m); break;
                    case TipoOperacion::RESERVA: r = atenderReserva(m); break;
                    case TipoOperacion::MODIFICACION: r = atenderModificacion(m); break;
                    default:
                        r = m;
                        r.resultado = Estado::ERROR;  // las cancelaciones no pasan por el RMC
                }
                logLinea("RMC", describir(m) + " -> " + nombre(r.resultado));
            }
            enviarMultipart(sockGT, {corr, serializar(r)});  // REP siempre debe responder
        }
        logLinea("RMC", "deteniendo");
    }

    // Consulta de los eventos de un mes: se reenvía a Persistencia y se devuelve su respuesta.
    Mensaje atenderConsulta(const Mensaje& m) {
        Mensaje r;
        if (!persistencia.pedir(m, r)) {
            r = m;
            r.resultado = Estado::ERROR;
            logLinea("RMC", "sin respuesta de Persistencia (timeout)");
        }
        return r;
    }

    // Reserva en dos pasos: (1) disponibilidad, (2) confirmación. El paso 2 vuelve a
    // verificar dentro de la transacción de la BD, así que no hay sobreventa.
    Mensaje atenderReserva(const Mensaje& m) {
        Mensaje disp = m;
        disp.tipo = TipoOperacion::CONSULTA;  // CONSULTA con mes == 0 -> disponibilidad
        disp.mes = 0;
        disp.eventos.clear();
        Mensaje rd;
        if (!persistencia.pedir(disp, rd) || rd.resultado == Estado::ERROR) {
            Mensaje r = m;
            r.resultado = Estado::ERROR;
            logLinea("RMC", "falló la consulta de disponibilidad");
            return r;
        }
        if (rd.resultado == Estado::RECHAZO || rd.eventos.empty() ||
            rd.eventos.front().asientosLibres < m.numAsientos) {
            Mensaje r = m;
            r.resultado = Estado::RECHAZO;  // evento inexistente o sin asientos suficientes
            return r;
        }
        Mensaje r;
        if (!persistencia.pedir(m, r)) {
            r = m;
            r.resultado = Estado::ERROR;
            logLinea("RMC", "sin respuesta de Persistencia al confirmar (timeout)");
        }
        return r;
    }

    // Segunda entrega.
    Mensaje atenderModificacion(const Mensaje& m) {
        Mensaje r = m;
        r.resultado = Estado::ERROR;
        return r;
    }

private:
    zmq::context_t ctx_;
    zmq::socket_t sockGT;
    EnlaceREQ persistencia;
};

int main(int argc, char** argv) {
    instalarSenales();
    try {
        ServicioRMC rmc(argStr(argc, argv, "--bind", "tcp://*:5556"),
                        argStr(argc, argv, "--persistencia", "tcp://localhost:5559"),
                        argInt(argc, argv, "--timeout-ms", 5000));
        rmc.iniciar();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
