// servicio_cancelaciones.cpp  (Máquina 2: capa de lógica de negocio)
// Servicio de Cancelaciones y Notificaciones. Recibe cancelaciones del GT por
// PUSH/PULL, ordena a Persistencia liberar los asientos (REQ/REP) y avisa al
// cliente por PUB/SUB con el id del cliente como tópico.
//
// Uso: ./cancelaciones [--bind-pull tcp://*:5557] [--bind-pub tcp://*:5558]
//                      [--persistencia tcp://localhost:5559] [--timeout-ms 5000]
#include "common.hpp"

using namespace srb;

class ServicioCancelaciones {
public:
    ServicioCancelaciones(const std::string& bindPull, const std::string& bindPub,
                          const std::string& epPersistencia, int timeoutMs)
        : ctx_(1),
          sockPull(crearSocket(ctx_, zmq::socket_type::pull)),
          sockPub(crearSocket(ctx_, zmq::socket_type::pub)),
          persistencia(ctx_, epPersistencia, timeoutMs) {
        sockPull.bind(bindPull);
        sockPub.bind(bindPub);
    }

    void iniciar() {
        logLinea("CANCELACIONES", "escuchando; esperando cancelaciones del GT");
        std::vector<std::string> tramas;
        while (!g_parar) {
            if (!hayEntrada(sockPull, 500)) continue;
            if (!recibirMultipart(sockPull, tramas) || tramas.empty()) continue;
            Mensaje m;
            if (!deserializar(tramas.back(), m)) {
                logLinea("CANCELACIONES", "mensaje mal formado, descartado");
                continue;
            }
            procesarCancelacion(m);  // las cancelaciones se atienden en orden de llegada
        }
        logLinea("CANCELACIONES", "deteniendo");
    }

    void procesarCancelacion(const Mensaje& m) {
        Mensaje r;
        if (!persistencia.pedir(m, r)) {
            r = m;
            r.resultado = Estado::ERROR;
            logLinea("CANCELACIONES", "sin respuesta de Persistencia (timeout)");
        }
        logLinea("CANCELACIONES", describir(m) + " -> " + nombre(r.resultado));
        notificarCliente(r);
    }

    // Tópico "c<idCliente>|": el separador evita que el cliente 1 reciba lo del 10.
    void notificarCliente(const Mensaje& r) {
        std::string topico = "c" + std::to_string(r.idCliente) + "|";
        enviarMultipart(sockPub, {topico, serializar(r)});
        logLinea("CANCELACIONES", "notificación enviada a cliente " + std::to_string(r.idCliente));
    }

private:
    zmq::context_t ctx_;
    zmq::socket_t sockPull;
    zmq::socket_t sockPub;
    EnlaceREQ persistencia;
};

int main(int argc, char** argv) {
    instalarSenales();
    try {
        ServicioCancelaciones c(argStr(argc, argv, "--bind-pull", "tcp://*:5557"),
                                argStr(argc, argv, "--bind-pub", "tcp://*:5558"),
                                argStr(argc, argv, "--persistencia", "tcp://localhost:5559"),
                                argInt(argc, argv, "--timeout-ms", 5000));
        c.iniciar();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
