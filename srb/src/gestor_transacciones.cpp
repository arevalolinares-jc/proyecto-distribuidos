// gestor_transacciones.cpp  (Máquina 2: capa de lógica de negocio)
// Puerta de entrada del sistema. Un solo hilo con zmq_poll sobre:
//   - ROUTER (clientes): recibe de los REQ de los clientes.
//   - DEALER (RMC): reenvía las operaciones síncronas y recibe sus respuestas.
// Así una solicitud lenta en el RMC no impide atender a los demás clientes.
// Las cancelaciones se aceptan de inmediato (ACK) y se envían por PUSH.
//
// Uso: ./gestor [--bind tcp://*:5555] [--rmc tcp://localhost:5556]
//               [--cancel tcp://localhost:5557]
#include <unordered_map>

#include "common.hpp"

using namespace srb;

class GestorTransacciones {
public:
    GestorTransacciones(const std::string& bind, const std::string& epRMC,
                        const std::string& epCancel)
        : ctx_(1),
          sockClientes(crearSocket(ctx_, zmq::socket_type::router)),
          sockRMC(crearSocket(ctx_, zmq::socket_type::dealer)),
          sockCancel(crearSocket(ctx_, zmq::socket_type::push)) {
        sockClientes.bind(bind);
        sockRMC.connect(epRMC);
        sockCancel.connect(epCancel);
    }

    void iniciar() {
        logLinea("GT", "escuchando clientes");
        zmq_pollitem_t items[2] = {{static_cast<void*>(sockClientes), 0, ZMQ_POLLIN, 0},
                                   {static_cast<void*>(sockRMC), 0, ZMQ_POLLIN, 0}};
        double ultimoResumen = ahoraSeg();
        long vistoAntes = 0;
        while (!g_parar) {
            int rc = zmq_poll(items, 2, 200);
            if (rc > 0) {
                if (items[0].revents & ZMQ_POLLIN) atenderCliente();
                if (items[1].revents & ZMQ_POLLIN) atenderRMC();
            }
            limpiarPendientes();
            if (ahoraSeg() - ultimoResumen >= 10.0) {
                ultimoResumen = ahoraSeg();
                long total = totalContadas();
                if (total != vistoAntes) {
                    vistoAntes = total;
                    imprimirResumen();
                }
            }
        }
        logLinea("GT", "deteniendo");
        imprimirResumen();
    }

    // Decide el canal: síncrono (RMC) o asíncrono (cancelaciones).
    void enrutar(const std::string& identidad, const Mensaje& m) {
        if (m.tipo == TipoOperacion::CANCELACION) {
            Mensaje ack = m;
            ack.resultado = Estado::EXITO;  // acuse de recibo inmediato
            responderCliente(identidad, ack);
            if (!enviarMultipart(sockCancel, {serializar(m)}))
                logLinea("GT", "no se pudo enviar la cancelación al servicio");
            logLinea("GT", describir(m) + " -> ACK inmediato, enviado a Cancelaciones");
            return;
        }
        unsigned long corr = ++secuencia;
        pendientes[corr] = {identidad, m.tipo, ahoraSeg()};
        if (tPrimera == 0.0) tPrimera = ahoraSeg();
        if (!enviarMultipart(sockRMC, {"", std::to_string(corr), serializar(m)})) {
            pendientes.erase(corr);
            Mensaje err = m;
            err.resultado = Estado::ERROR;
            responderCliente(identidad, err);
            contarTransaccion(m.tipo, Estado::ERROR);
        }
    }

    void responderCliente(const std::string& identidad, const Mensaje& m) {
        enviarMultipart(sockClientes, {identidad, "", serializar(m)});
    }

    // Se cuenta al responder (transacciones procesadas), separando el resultado.
    void contarTransaccion(TipoOperacion tipo, Estado resultado) {
        contadores[static_cast<int>(tipo)][static_cast<int>(resultado)]++;
        tUltima = ahoraSeg();
    }

private:
    struct Pendiente {
        std::string identidad;
        TipoOperacion tipo;
        double t0;
    };

    void atenderCliente() {
        std::vector<std::string> f;
        if (!recibirMultipart(sockClientes, f) || f.size() < 2) return;
        std::string identidad = f.front();
        std::string payload = f.back();  // el REQ agrega un delimitador vacío en medio

        Mensaje m;
        std::string motivo;
        if (payload.size() > kMaxBytes || !deserializar(payload, m)) {
            Mensaje err;
            err.resultado = Estado::ERROR;
            responderCliente(identidad, err);
            logLinea("GT", "mensaje mal formado o demasiado grande -> ERROR");
            return;
        }
        if (!validar(m, motivo)) {
            Mensaje err = m;
            err.resultado = Estado::ERROR;
            responderCliente(identidad, err);
            logLinea("GT", describir(m) + " inválido (" + motivo + ") -> ERROR");
            return;
        }
        enrutar(identidad, m);
    }

    void atenderRMC() {
        std::vector<std::string> f;
        if (!recibirMultipart(sockRMC, f) || f.size() < 3) return;  // ["", corr, mensaje]
        unsigned long corr = 0;
        try {
            corr = std::stoul(f[1]);
        } catch (...) {
            return;
        }
        auto it = pendientes.find(corr);
        if (it == pendientes.end()) {
            logLinea("GT", "respuesta tardía del RMC descartada");
            return;
        }
        Pendiente p = it->second;
        pendientes.erase(it);
        Mensaje r;
        if (!deserializar(f[2], r)) {
            r.resultado = Estado::ERROR;
        }
        responderCliente(p.identidad, r);
        contarTransaccion(p.tipo, r.resultado);
        char t[32];
        std::snprintf(t, sizeof t, "%.1f ms", (ahoraSeg() - p.t0) * 1000.0);
        logLinea("GT", describir(r) + " -> " + nombre(r.resultado) + " (" + t + ")");
    }

    // Validación básica antes de reenviar a otros componentes.
    static bool validar(const Mensaje& m, std::string& motivo) {
        switch (m.tipo) {
            case TipoOperacion::CONSULTA: {
                int anio = m.mes / 100, mes = m.mes % 100;
                if (anio < 2000 || anio > 2100 || mes < 1 || mes > 12) {
                    motivo = "mes fuera de rango (AAAAMM)";
                    return false;
                }
                return true;
            }
            case TipoOperacion::RESERVA:
                if (m.idEvento < 1 || m.idEvento > 100000 || m.idOcurrencia < 1 ||
                    m.idOcurrencia > 100) {
                    motivo = "evento/ocurrencia fuera de rango";
                    return false;
                }
                if (m.numAsientos < 1 || m.numAsientos > 100) {
                    motivo = "numAsientos fuera de rango";
                    return false;
                }
                return true;
            case TipoOperacion::CANCELACION:
                if (m.idReserva < 1) {
                    motivo = "idReserva inválido";
                    return false;
                }
                return true;
            case TipoOperacion::MODIFICACION: return true;
        }
        return false;
    }

    // Descarta solicitudes que llevan más de un minuto sin respuesta del RMC.
    void limpiarPendientes() {
        double ahora = ahoraSeg();
        for (auto it = pendientes.begin(); it != pendientes.end();) {
            if (ahora - it->second.t0 > 60.0) it = pendientes.erase(it);
            else ++it;
        }
    }

    long totalContadas() const {
        long t = 0;
        for (int i = 0; i < 4; ++i)
            for (int j = 0; j < 3; ++j) t += contadores[i][j];
        return t;
    }

    void imprimirResumen() {
        double minutos = (tUltima > tPrimera) ? (tUltima - tPrimera) / 60.0 : 0.0;
        for (int i = 0; i < 4; ++i) {
            long n = contadores[i][0] + contadores[i][1] + contadores[i][2];
            if (n == 0) continue;
            char b[160];
            std::snprintf(b, sizeof b, "%s: total=%ld exito=%ld rechazo=%ld error=%ld  TPM=%.1f",
                          nombre(static_cast<TipoOperacion>(i)), n, contadores[i][0],
                          contadores[i][1], contadores[i][2], minutos > 0 ? n / minutos : 0.0);
            logLinea("GT-STATS", b);
        }
    }

    static constexpr size_t kMaxBytes = 4096;

    zmq::context_t ctx_;
    zmq::socket_t sockClientes;  // ROUTER
    zmq::socket_t sockRMC;       // DEALER
    zmq::socket_t sockCancel;    // PUSH
    std::unordered_map<unsigned long, Pendiente> pendientes;
    unsigned long secuencia = 0;
    long contadores[4][3] = {};
    double tPrimera = 0.0, tUltima = 0.0;
};

int main(int argc, char** argv) {
    instalarSenales();
    try {
        GestorTransacciones gt(argStr(argc, argv, "--bind", "tcp://*:5555"),
                               argStr(argc, argv, "--rmc", "tcp://localhost:5556"),
                               argStr(argc, argv, "--cancel", "tcp://localhost:5557"));
        gt.iniciar();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
