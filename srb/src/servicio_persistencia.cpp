// servicio_persistencia.cpp  (Máquina 3: capa de almacenamiento)
// Único componente que habla con PostgreSQL. Atiende por un socket REP las
// solicitudes del RMC y del Servicio de Cancelaciones.
//
// Uso:
//   ./persistencia [--bind tcp://*:5559] [--db "dbname=srb"] [--reset]
//                  [--mes 202611] [--eventos 50] [--dobles 20] [--asientos 100]
//                  [--volcar-cada SEGUNDOS]
//   kill -USR1 <pid>   imprime el estado completo de la BD
#include <pqxx/pqxx>

#include <memory>

#include "common.hpp"

using namespace srb;

class ServicioPersistencia {
public:
    ServicioPersistencia(const std::string& bind, const std::string& cadenaBD)
        : cadenaBD_(cadenaBD), ctx_(1), sockRep(crearSocket(ctx_, zmq::socket_type::rep)) {
        conectarBD();
        sockRep.bind(bind);
    }

    // Borra y recrea las tablas y carga los eventos iniciales.
    void cargarDatosIniciales(int numEventos, int numDobles, int asientos, int mes) {
        pqxx::work tx(*conexion);
        tx.exec("DROP TABLE IF EXISTS reserva");
        tx.exec("DROP TABLE IF EXISTS evento");
        tx.exec(
            "CREATE TABLE evento ("
            " id_evento INT NOT NULL,"
            " id_ocurrencia INT NOT NULL,"
            " num_ocurrencias INT NOT NULL,"
            " fecha INT NOT NULL,"  // AAAAMMDD
            " asientos_libres INT NOT NULL CHECK (asientos_libres >= 0),"
            " asientos_ocupados INT NOT NULL CHECK (asientos_ocupados >= 0),"
            " PRIMARY KEY (id_evento, id_ocurrencia))");
        tx.exec(
            "CREATE TABLE reserva ("
            " id_reserva SERIAL PRIMARY KEY,"
            " id_cliente INT NOT NULL,"
            " id_evento INT NOT NULL,"
            " id_ocurrencia INT NOT NULL,"
            " num_asientos INT NOT NULL CHECK (num_asientos > 0),"
            " activa BOOLEAN NOT NULL DEFAULT TRUE,"
            " FOREIGN KEY (id_evento, id_ocurrencia) REFERENCES evento (id_evento, id_ocurrencia))");
        tx.exec("CREATE INDEX evento_fecha_idx ON evento (fecha)");
        for (int e = 1; e <= numEventos; ++e) {
            int ocurrencias = (e <= numDobles) ? 2 : 1;
            for (int o = 1; o <= ocurrencias; ++o) {
                // Días 1..14 para la primera ocurrencia y 15..28 para la segunda.
                int dia = (o == 1 ? 1 : 15) + (e % 14);
                int fecha = mes * 100 + dia;
                tx.exec_params(
                    "INSERT INTO evento (id_evento, id_ocurrencia, num_ocurrencias, fecha,"
                    " asientos_libres, asientos_ocupados) VALUES ($1, $2, $3, $4, $5, 0)",
                    e, o, ocurrencias, fecha, asientos);
            }
        }
        tx.commit();
        logLinea("PERSISTENCIA", "BD cargada: " + std::to_string(numEventos) + " eventos (" +
                                     std::to_string(numDobles) + " con dos ocurrencias), " +
                                     std::to_string(asientos) + " asientos por ocurrencia, mes " +
                                     std::to_string(mes));
    }

    bool existeEsquema() {
        pqxx::work tx(*conexion);
        pqxx::result r = tx.exec("SELECT to_regclass('evento') IS NOT NULL");
        tx.commit();
        return r[0][0].as<bool>();
    }

    void iniciar(int volcarCada) {
        logLinea("PERSISTENCIA", "escuchando; esperando solicitudes");
        double ultimoVolcado = ahoraSeg();
        std::vector<std::string> tramas;
        while (!g_parar) {
            if (g_volcar) {
                g_volcar = 0;
                volcarEstado(true);
            }
            if (volcarCada > 0 && ahoraSeg() - ultimoVolcado >= volcarCada) {
                ultimoVolcado = ahoraSeg();
                volcarEstado(false);
            }
            if (!hayEntrada(sockRep, 500)) continue;
            if (!recibirMultipart(sockRep, tramas)) continue;

            Mensaje m, r;
            if (tramas.empty() || !deserializar(tramas.back(), m)) {
                r.resultado = Estado::ERROR;
                logLinea("PERSISTENCIA", "mensaje mal formado -> ERROR");
            } else {
                r = atender(m);
                logLinea("PERSISTENCIA", describir(m) + " -> " + nombre(r.resultado) +
                                             (r.idReserva > 0 && m.tipo == TipoOperacion::RESERVA
                                                  ? " reserva=" + std::to_string(r.idReserva)
                                                  : ""));
            }
            enviarMultipart(sockRep, {serializar(r)});  // REP siempre debe responder
        }
        logLinea("PERSISTENCIA", "deteniendo");
        volcarEstado(true);
    }

    std::vector<Evento> consultarEventos(int mes) {
        pqxx::work tx(*conexion);
        pqxx::result r = tx.exec_params(
            "SELECT id_evento, id_ocurrencia, num_ocurrencias, fecha, asientos_libres,"
            " asientos_ocupados FROM evento WHERE fecha / 100 = $1"
            " ORDER BY id_evento, id_ocurrencia",
            mes);
        tx.commit();
        std::vector<Evento> v;
        for (auto fila : r) {
            Evento e;
            e.idEvento = fila[0].as<int>();
            e.idOcurrencia = fila[1].as<int>();
            e.numOcurrencias = fila[2].as<int>();
            e.fecha = fila[3].as<int>();
            e.asientosLibres = fila[4].as<int>();
            e.asientosOcupados = fila[5].as<int>();
            v.push_back(e);
        }
        return v;
    }

    // Asientos libres de una ocurrencia, o -1 si no existe.
    int consultarDisponibilidad(int idEvento, int idOcurrencia) {
        pqxx::work tx(*conexion);
        pqxx::result r = tx.exec_params(
            "SELECT asientos_libres FROM evento WHERE id_evento = $1 AND id_ocurrencia = $2",
            idEvento, idOcurrencia);
        tx.commit();
        if (r.empty()) return -1;
        return r[0][0].as<int>();
    }

    // Verifica y confirma en una sola transacción: el UPDATE solo afecta la fila si
    // aún hay asientos suficientes, así dos clientes no pueden quedarse con el último.
    Mensaje reservarAsientos(const Mensaje& m) {
        Mensaje r = m;
        pqxx::work tx(*conexion);
        pqxx::result u = tx.exec_params(
            "UPDATE evento SET asientos_libres = asientos_libres - $1,"
            " asientos_ocupados = asientos_ocupados + $1"
            " WHERE id_evento = $2 AND id_ocurrencia = $3 AND asientos_libres >= $1",
            m.numAsientos, m.idEvento, m.idOcurrencia);
        if (u.affected_rows() == 0) {
            r.resultado = Estado::RECHAZO;  // sin cupo (o el evento no existe); se deshace
            return r;
        }
        pqxx::result ins = tx.exec_params(
            "INSERT INTO reserva (id_cliente, id_evento, id_ocurrencia, num_asientos)"
            " VALUES ($1, $2, $3, $4) RETURNING id_reserva",
            m.idCliente, m.idEvento, m.idOcurrencia, m.numAsientos);
        r.idReserva = ins[0][0].as<int>();
        tx.commit();
        r.resultado = Estado::EXITO;
        return r;
    }

    // Segunda entrega.
    Mensaje modificarReserva(const Mensaje& m) {
        Mensaje r = m;
        r.resultado = Estado::ERROR;
        logLinea("PERSISTENCIA", "MODIFICACION no implementada en la primera entrega");
        return r;
    }

    // Marca la reserva como inactiva y devuelve sus asientos al evento.
    Mensaje liberarAsientos(const Mensaje& m) {
        Mensaje r = m;
        pqxx::work tx(*conexion);
        pqxx::result u = tx.exec_params(
            "UPDATE reserva SET activa = FALSE"
            " WHERE id_reserva = $1 AND id_cliente = $2 AND activa"
            " RETURNING id_evento, id_ocurrencia, num_asientos",
            m.idReserva, m.idCliente);
        if (u.empty()) {
            r.resultado = Estado::RECHAZO;  // no existe, no es del cliente o ya estaba cancelada
            return r;
        }
        int ev = u[0][0].as<int>(), oc = u[0][1].as<int>(), n = u[0][2].as<int>();
        tx.exec_params(
            "UPDATE evento SET asientos_libres = asientos_libres + $1,"
            " asientos_ocupados = asientos_ocupados - $1"
            " WHERE id_evento = $2 AND id_ocurrencia = $3",
            n, ev, oc);
        tx.commit();
        r.idEvento = ev;
        r.idOcurrencia = oc;
        r.numAsientos = n;
        r.resultado = Estado::EXITO;
        return r;
    }

    // Imprime el estado de la BD (resumen o detalle por ocurrencia).
    void volcarEstado(bool detalle) {
        try {
            pqxx::work tx(*conexion);
            pqxx::result ev = tx.exec(
                "SELECT id_evento, id_ocurrencia, num_ocurrencias, fecha, asientos_libres,"
                " asientos_ocupados FROM evento ORDER BY id_evento, id_ocurrencia");
            pqxx::result rs = tx.exec("SELECT COUNT(*) FROM reserva WHERE activa");
            tx.commit();
            long libres = 0, ocupados = 0;
            for (auto f : ev) {
                libres += f[4].as<int>();
                ocupados += f[5].as<int>();
                if (detalle)
                    logLinea("BD", "E" + std::to_string(f[0].as<int>()) + " occ" +
                                       std::to_string(f[1].as<int>()) + "/" +
                                       std::to_string(f[2].as<int>()) +
                                       " fecha=" + std::to_string(f[3].as<int>()) +
                                       " libres=" + std::to_string(f[4].as<int>()) +
                                       " ocupados=" + std::to_string(f[5].as<int>()));
            }
            logLinea("BD", "TOTAL ocurrencias=" + std::to_string(ev.size()) +
                               " libres=" + std::to_string(libres) +
                               " ocupados=" + std::to_string(ocupados) +
                               " reservas_activas=" + std::to_string(rs[0][0].as<long>()));
        } catch (const std::exception& e) {
            logLinea("BD", std::string("no se pudo volcar el estado: ") + e.what());
        }
    }

private:
    void conectarBD() { conexion.reset(new pqxx::connection(cadenaBD_)); }

    // Despacha según el tipo. Cualquier fallo de BD se convierte en ERROR.
    Mensaje atender(const Mensaje& m) {
        try {
            switch (m.tipo) {
                case TipoOperacion::CONSULTA: {
                    Mensaje r = m;
                    r.resultado = Estado::EXITO;
                    if (m.mes > 0) {
                        r.eventos = consultarEventos(m.mes);
                    } else {
                        int libres = consultarDisponibilidad(m.idEvento, m.idOcurrencia);
                        if (libres < 0) {
                            r.resultado = Estado::RECHAZO;  // el evento no existe
                        } else {
                            Evento e;
                            e.idEvento = m.idEvento;
                            e.idOcurrencia = m.idOcurrencia;
                            e.asientosLibres = libres;
                            r.eventos = {e};
                        }
                    }
                    return r;
                }
                case TipoOperacion::RESERVA: return reservarAsientos(m);
                case TipoOperacion::MODIFICACION: return modificarReserva(m);
                case TipoOperacion::CANCELACION: return liberarAsientos(m);
            }
        } catch (const pqxx::broken_connection& e) {
            logLinea("PERSISTENCIA", std::string("conexión a la BD perdida: ") + e.what());
            try {
                conectarBD();
            } catch (...) {
            }
        } catch (const std::exception& e) {
            logLinea("PERSISTENCIA", std::string("error de BD: ") + e.what());
        }
        Mensaje r = m;
        r.resultado = Estado::ERROR;
        return r;
    }

    std::string cadenaBD_;
    zmq::context_t ctx_;
    zmq::socket_t sockRep;
    std::unique_ptr<pqxx::connection> conexion;
};

int main(int argc, char** argv) {
    instalarSenales();
    std::string bind = argStr(argc, argv, "--bind", "tcp://*:5559");
    std::string bd = argStr(argc, argv, "--db", "dbname=srb");
    try {
        ServicioPersistencia p(bind, bd);
        if (argFlag(argc, argv, "--reset") || !p.existeEsquema())
            p.cargarDatosIniciales(argInt(argc, argv, "--eventos", 50),
                                   argInt(argc, argv, "--dobles", 20),
                                   argInt(argc, argv, "--asientos", 100),
                                   argInt(argc, argv, "--mes", 202611));
        p.volcarEstado(true);  // estado inicial
        p.iniciar(argInt(argc, argv, "--volcar-cada", 0));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Error fatal: %s\n", e.what());
        return 1;
    }
    return 0;
}
