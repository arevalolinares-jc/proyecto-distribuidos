# Makefile del SRB (arquitectura base)
# Requiere: g++ (C++17), libzmq3-dev, cppzmq (zmq.hpp), libpqxx-dev, libpq-dev.
CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pthread -Isrc
BIN       = bin

all: $(BIN)/persistencia $(BIN)/rmc $(BIN)/cancelaciones $(BIN)/gestor $(BIN)/generador

$(BIN):
	mkdir -p $(BIN)

$(BIN)/persistencia: src/servicio_persistencia.cpp src/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ -lzmq -lpqxx -lpq

$(BIN)/rmc: src/servicio_rmc.cpp src/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ -lzmq

$(BIN)/cancelaciones: src/servicio_cancelaciones.cpp src/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ -lzmq

$(BIN)/gestor: src/gestor_transacciones.cpp src/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ -lzmq

$(BIN)/generador: src/generador_carga.cpp src/common.hpp | $(BIN)
	$(CXX) $(CXXFLAGS) $< -o $@ -lzmq

clean:
	rm -rf $(BIN) logs

.PHONY: all clean
