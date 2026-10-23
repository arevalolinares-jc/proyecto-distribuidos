// generador_carga.cpp  (Máquina 1: capa de consumidores)
// GeneradorCarga crea un hilo por Cliente. Cada Cliente envía solicitudes al
// Gestor de Transacciones (REQ) y escucha notificaciones (SUB). Metricas
// guarda los tiempos de respuesta medidos con el reloj del propio cliente.
//
// Uso: ./generador --clientes 3 --solo-consulta 2 --rpm 60 --duracion 120
//        [--gt tcp://localhost:5555] [--notif tcp://localhost:5558]
//        [--pct-consulta 70 --pct-reserva 30 --pct-cancelar 0]   (clientes mixtos)
//        [--mes 202611] [--eventos 50] [--dobles 20] [--max-asientos 4]
//        [--calentamiento 5] [--timeout-ms 5000] [--semilla 1]
//        [--arquitectura base] [--csv resultados.csv]
//   --rpm 0  = sin pausa entre solicitudes (máxima carga).
#include <algorithm>
#include <cmath>
#include <fstream>
#include <random>
#include <thread>

#include "common.hpp"

using namespace srb;

// ---------------------------------------------------------------- Metricas --
class Metricas {
public:
    // Registra el tiempo de respuesta (ms) de una operación completada.
    void registrar(TipoOperacion tipo, double ms) {
        std::lock_guard<std::mutex> g(mtx);
        if (tipo == TipoOperacion::CONSULTA) {
            tiemposConsulta.push_back(ms);
            contadorConsultas++;
        } else {
            tiemposReserva.push_back(ms);
            contadorReservas++;
        }
    }
    void registrarResultado(TipoOperacion tipo, Estado e) {
        std::lock_guard<std::mutex> g(mtx);
        if (e == Estado::RECHAZO) rechazos[idx(tipo)]++;
        else if (e == Estado::ERROR) errores[idx(tipo)]++;
    }
    void registrarTimeout(TipoOperacion tipo) {
        std::lock_guard<std::mutex> g(mtx);
        timeouts[idx(tipo)]++;  // aparte, para no distorsionar los promedios
    }
    void registrarCancelacion() {
        std::lock_guard<std::mutex> g(mtx);
        cancelaciones++;
    }
    void registrarNotificacion() {
        std::lock_guard<std::mutex> g(mtx);
        notificaciones++;
    }

    size_t n(TipoOperacion t) const {
        std::lock_guard<std::mutex> g(mtx);
        return vec(t).size();
    }
    double promedio(TipoOperacion t) const {
        std::lock_guard<std::mutex> g(mtx);
        const auto& v = vec(t);
        if (v.empty()) return 0.0;
        double s = 0;
        for (double x : v) s += x;
        return s / v.size();
    }
    // Desviación estándar muestral (n-1).
    double desviacionEstandar(TipoOperacion t) const {
        double p = promedio(t);
        std::lock_guard<std::mutex> g(mtx);
        const auto& v = vec(t);
        if (v.size() < 2) return 0.0;
        double s = 0;
        for (double x : v) s += (x - p) * (x - p);
        return std::sqrt(s / (v.size() - 1));
    }
    double percentil95(TipoOperacion t) const {
        std::lock_guard<std::mutex> g(mtx);
        std::vector<double> v = vec(t);
        if (v.empty()) return 0.0;
        std::sort(v.begin(), v.end());
        size_t i = static_cast<size_t>(std::ceil(0.95 * v.size())) - 1;
        return v[std::min(i, v.size() - 1)];
    }
    // Transacciones por minuto vistas por el generador en la ventana medida.
    double tpm(TipoOperacion t, double duracionSeg) const {
        if (duracionSeg <= 0) return 0.0;
        return n(t) / (duracionSeg / 60.0);
    }
    long getRechazos(TipoOperacion t) const { std::lock_guard<std::mutex> g(mtx); return rechazos[idx(t)]; }
    long getErrores(TipoOperacion t) const { std::lock_guard<std::mutex> g(mtx); return errores[idx(t)]; }
    long getTimeouts(TipoOperacion t) const { std::lock_guard<std::mutex> g(mtx); return timeouts[idx(t)]; }
    long getCancelaciones() const { std::lock_guard<std::mutex> g(mtx); return cancelaciones; }
    long getNotificaciones() const { std::lock_guard<std::mutex> g(mtx); return notificaciones; }

private:
    static int idx(TipoOperacion t) { return t == TipoOperacion::CONSULTA ? 0 : 1; }
    const std::vector<double>& vec(TipoOperacion t) const {
        return t == TipoOperacion::CONSULTA ? tiemposConsulta : tiemposReserva;
    }
    mutable std::mutex mtx;
    std::vector<double> tiemposConsulta, tiemposReserva;
    int contadorConsultas = 0, contadorReservas = 0;
    long rechazos[2] = {0, 0}, errores[2] = {0, 0}, timeouts[2] = {0, 0};
    long cancelaciones = 0, notificaciones = 0;
};

// ----------------------------------------------------------------- Cliente --
class Cliente {
public:
    Cliente(zmq::context_t& ctx, int id, const std::string& epGT, const std::string& epNotif,
            int timeoutMs)
        : idCliente(id),
          sockReq(ctx, epGT, timeoutMs),
          sockSub(crearSocket(ctx, zmq::socket_type::sub)) {
        if (!epNotif.empty()) {
            sockSub.connect(epNotif);
            std::string topico = "c" + std::to_string(id) + "|";
            sockSub.set(zmq::sockopt::subscribe, topico);
        }
    }

    // false = no llegó respuesta a tiempo.
    bool enviarSolicitud(const Mensaje& m, Mensaje& respuesta) { return sockReq.pedir(m, respuesta); }

    // Espera hasta esperaMs por una notificación de cancelación efectiva.
    bool recibirNotificacion(Mensaje& out, int esperaMs) {
        if (!hayEntrada(sockSub, esperaMs)) return false;
        std::vector<std::string> t;
        if (!recibirMultipart(sockSub, t) || t.size() < 2) return false;
        return deserializar(t.back(), out);
    }

    int idCliente;

private:
    EnlaceREQ sockReq;
    zmq::socket_t sockSub;
};

// ----------------------------------------------------------- GeneradorCarga --
struct Parametros {
    std::string gt, notif, arquitectura, csv;
    int clientes, soloConsulta, rpm, duracion, calentamiento, timeoutMs, semilla;
    int pctConsulta, pctReserva, pctCancelar;
    int mes, eventos, dobles, maxAsientos;
};

class GeneradorCarga {
public:
    explicit GeneradorCarga(const Parametros& p) : p_(p), ctx_(1) {}

    void iniciar() {
        logLinea("CARGA", "clientes=" + std::to_string(p_.clientes) + " (solo consulta: " +
                              std::to_string(p_.soloConsulta) + ") rpm=" + std::to_string(p_.rpm) +
                              " duracion=" + std::to_string(p_.duracion) + "s calentamiento=" +
                              std::to_string(p_.calentamiento) + "s");
        t0_ = ahoraSeg();
        std::vector<std::thread> hilos;
        for (int i = 0; i < p_.clientes; ++i) hilos.emplace_back(&GeneradorCarga::cicloCliente, this, i);
        for (auto& h : hilos) h.join();
        informar();
    }

    // Construye la solicitud de un cliente según su perfil.
    Mensaje generarSolicitud(int idCliente, int seq, bool soloConsulta, std::mt19937& rng,
                             std::vector<int>& misReservas) {
        Mensaje m;
        m.idMensaje = idCliente * 100000 + seq;
        m.idCliente = idCliente;
        m.endpointCliente = "sub:" + p_.notif;
        m.tsEnvio = relojPared();

        int dado = static_cast<int>(rng() % 100);
        TipoOperacion t = TipoOperacion::CONSULTA;
        if (!soloConsulta) {
            if (dado < p_.pctConsulta) t = TipoOperacion::CONSULTA;
            else if (dado < p_.pctConsulta + p_.pctReserva) t = TipoOperacion::RESERVA;
            else t = misReservas.empty() ? TipoOperacion::RESERVA : TipoOperacion::CANCELACION;
        }
        m.tipo = t;
        if (t == TipoOperacion::CONSULTA) {
            m.mes = p_.mes;
        } else if (t == TipoOperacion::RESERVA) {
            m.idEvento = 1 + static_cast<int>(rng() % p_.eventos);
            m.idOcurrencia = (m.idEvento <= p_.dobles) ? 1 + static_cast<int>(rng() % 2) : 1;
            m.numAsientos = 1 + static_cast<int>(rng() % p_.maxAsientos);
        } else {  // CANCELACION de una reserva propia
            size_t i = rng() % misReservas.size();
            m.idReserva = misReservas[i];
            misReservas.erase(misReservas.begin() + i);
        }
        return m;
    }

private:
    void cicloCliente(int idx) {
        const int idCliente = idx + 1;
        const bool soloConsulta = idx < p_.soloConsulta;
        Cliente cliente(ctx_, idCliente, p_.gt, p_.notif, p_.timeoutMs);
        std::mt19937 rng(static_cast<unsigned>(p_.semilla) * 7919u + idCliente);
        std::vector<int> misReservas;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));  // que el SUB se conecte

        const double intervalo = p_.rpm > 0 ? 60.0 / p_.rpm : 0.0;
        const double fin = t0_ + p_.duracion;
        int seq = 0;
        while (ahoraSeg() < fin) {
            double inicioIter = ahoraSeg();
            Mensaje m = generarSolicitud(idCliente, ++seq, soloConsulta, rng, misReservas);
            Mensaje r;
            double ts = ahoraSeg();
            bool ok = cliente.enviarSolicitud(m, r);
            double ms = (ahoraSeg() - ts) * 1000.0;
            bool medir = (ts - t0_) >= p_.calentamiento;  // se descartan los primeros segundos

            if (m.tipo == TipoOperacion::CANCELACION) {
                if (ok && medir) metricas.registrarCancelacion();
            } else if (medir) {
                if (!ok) {
                    metricas.registrarTimeout(m.tipo);
                } else {
                    metricas.registrar(m.tipo, ms);
                    metricas.registrarResultado(m.tipo, r.resultado);
                }
            }
            if (ok && m.tipo == TipoOperacion::RESERVA && r.resultado == Estado::EXITO)
                misReservas.push_back(r.idReserva);

            Mensaje nota;
            while (cliente.recibirNotificacion(nota, 0)) metricas.registrarNotificacion();

            double restante = intervalo - (ahoraSeg() - inicioIter);
            if (restante > 0) std::this_thread::sleep_for(std::chrono::duration<double>(restante));
        }
        // Espera breve por notificaciones que aún estén en camino.
        Mensaje nota;
        double limite = ahoraSeg() + 2.0;
        while (ahoraSeg() < limite) {
            if (cliente.recibirNotificacion(nota, 200)) metricas.registrarNotificacion();
            else if (p_.pctCancelar == 0) break;
        }
    }

    void informar() {
        const double ventana = std::max(1.0, static_cast<double>(p_.duracion - p_.calentamiento));
        const TipoOperacion C = TipoOperacion::CONSULTA, R = TipoOperacion::RESERVA;
        char b[256];
        std::snprintf(b, sizeof b, "CONSULTA: n=%zu prom=%.2f ms std=%.2f p95=%.2f TPM=%.1f timeouts=%ld",
                      metricas.n(C), metricas.promedio(C), metricas.desviacionEstandar(C),
                      metricas.percentil95(C), metricas.tpm(C, ventana), metricas.getTimeouts(C));
        logLinea("CARGA", b);
        std::snprintf(b, sizeof b,
                      "RESERVA:  n=%zu prom=%.2f ms std=%.2f p95=%.2f TPM=%.1f rechazos=%ld timeouts=%ld",
                      metricas.n(R), metricas.promedio(R), metricas.desviacionEstandar(R),
                      metricas.percentil95(R), metricas.tpm(R, ventana), metricas.getRechazos(R),
                      metricas.getTimeouts(R));
        logLinea("CARGA", b);
        if (p_.pctCancelar > 0)
            logLinea("CARGA", "cancelaciones enviadas=" + std::to_string(metricas.getCancelaciones()) +
                                  " notificaciones recibidas=" +
                                  std::to_string(metricas.getNotificaciones()));

        if (p_.csv.empty()) return;
        bool existe = std::ifstream(p_.csv).good();
        std::ofstream f(p_.csv, std::ios::app);
        if (!existe)
            f << "arquitectura,clientes,solo_consulta,duracion_s,calentamiento_s,"
                 "consultas_n,consulta_prom_ms,consulta_std_ms,consulta_p95_ms,consulta_tpm,"
                 "consulta_timeouts,reservas_n,reserva_prom_ms,reserva_std_ms,reserva_p95_ms,"
                 "reserva_tpm,reserva_rechazos,reserva_timeouts,cancelaciones,notificaciones\n";
        char fila[512];
        std::snprintf(fila, sizeof fila,
                      "%s,%d,%d,%d,%d,%zu,%.3f,%.3f,%.3f,%.2f,%ld,%zu,%.3f,%.3f,%.3f,%.2f,%ld,%ld,%ld,%ld\n",
                      p_.arquitectura.c_str(), p_.clientes, p_.soloConsulta, p_.duracion,
                      p_.calentamiento, metricas.n(C), metricas.promedio(C),
                      metricas.desviacionEstandar(C), metricas.percentil95(C), metricas.tpm(C, ventana),
                      metricas.getTimeouts(C), metricas.n(R), metricas.promedio(R),
                      metricas.desviacionEstandar(R), metricas.percentil95(R), metricas.tpm(R, ventana),
                      metricas.getRechazos(R), metricas.getTimeouts(R), metricas.getCancelaciones(),
                      metricas.getNotificaciones());
        f << fila;
        logLinea("CARGA", "fila agregada a " + p_.csv);
    }

    Parametros p_;
    zmq::context_t ctx_;
    Metricas metricas;
    double t0_ = 0.0;
};

int main(int argc, char** argv) {
    Parametros p;
    p.gt = argStr(argc, argv, "--gt", "tcp://localhost:5555");
    p.notif = argStr(argc, argv, "--notif", "tcp://localhost:5558");
    p.arquitectura = argStr(argc, argv, "--arquitectura", "base");
    p.csv = argStr(argc, argv, "--csv", "");
    p.clientes = argInt(argc, argv, "--clientes", 3);
    p.soloConsulta = argInt(argc, argv, "--solo-consulta", 2);
    p.rpm = argInt(argc, argv, "--rpm", 60);
    p.duracion = argInt(argc, argv, "--duracion", 120);
    p.calentamiento = argInt(argc, argv, "--calentamiento", 5);
    p.timeoutMs = argInt(argc, argv, "--timeout-ms", 5000);
    p.semilla = argInt(argc, argv, "--semilla", 1);
    p.pctConsulta = argInt(argc, argv, "--pct-consulta", 70);
    p.pctReserva = argInt(argc, argv, "--pct-reserva", 30);
    p.pctCancelar = argInt(argc, argv, "--pct-cancelar", 0);
    p.mes = argInt(argc, argv, "--mes", 202611);
    p.eventos = std::max(1, argInt(argc, argv, "--eventos", 50));
    p.dobles = argInt(argc, argv, "--dobles", 20);
    p.maxAsientos = std::max(1, argInt(argc, argv, "--max-asientos", 4));
    if (p.pctConsulta + p.pctReserva + p.pctCancelar != 100) {
        std::fprintf(stderr, "Los porcentajes (consulta+reserva+cancelar) deben sumar 100\n");
        return 1;
    }
    if (p.clientes < 1 || p.soloConsulta > p.clientes) {
        std::fprintf(stderr, "Revise --clientes y --solo-consulta\n");
        return 1;
    }
    GeneradorCarga g(p);
    g.iniciar();
    return 0;
}
