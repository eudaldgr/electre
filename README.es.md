# Electre

Servidor Electrum (protocolo 1.6) implementado en .NET 10, con Bitcoin Core como fuente de verdad y RocksDB como almacén de indexación.

## Objetivo del proyecto

Electre está pensado para dar respuesta rápida a consultas Electrum (historial, UTXO, mempool y cabeceras) manteniendo un modelo operativo simple: un único servicio, configuración explícita y componentes separados por responsabilidad.

## Decisiones técnicas tomadas

- **.NET 10 + Generic Host**: permite un modelo sólido de servicios en background (`Syncer`, `TcpServer`, métricas, health checks) con inyección de dependencias estándar.
- **RocksDB como base de indexación**: elegida por su throughput de escritura y latencia de lectura en carga alta; se trabaja con column families para separar dominios (headers, UTXO, historial, metadata, undo, etc.).
- **Bitcoin Core vía RPC**: la cadena y la mempool se leen desde Core para evitar divergencias de consenso y reducir complejidad de validación propia.
- **Sincronización orientada a I/O**: descarga paralela de bloques (`BlockDownloader`) e indexación secuencial/consistente (`Syncer`) para combinar velocidad y corrección.
- **Cache local para hot paths**: cache de UTXO y caches LRU para reducir consultas repetidas y estabilizar latencia en carga real.
- **Observabilidad desde el primer día**: logging con Serilog, endpoint de métricas (Prometheus) y health checks para facilitar operación y diagnóstico.
- **Configuración centralizada**: toda la parametrización importante pasa por `Config`/`appsettings.json` (RPC, límites, puertos, ritmos de polling, tuning de DB).

## Arquitectura (resumen)

- `Program.cs`: bootstrap, DI y registro de servicios.
- `Indexer/Syncer.cs`: bucle principal de sincronización y reorg.
- `Bitcoin/RpcClient.cs`: cliente RPC resiliente hacia Bitcoin Core.
- `Database/Store.cs`: capa de acceso a RocksDB.
- `Database/UtxoCache.cs`: cache de UTXO para rutas críticas.
- `Protocol/Methods.cs`: handlers JSON-RPC Electrum.
- `Server/TcpServer.cs` y sesiones: gestión de conexiones cliente.

## Ejecución rápida

### 1) Requisitos

- .NET SDK 10
- Bitcoin Core con RPC habilitado

### 2) Configuración

Edita `src/Electre/appsettings.json` (mínimo):

- `BitcoinRpcUrl`
- `BitcoinRpcUser`
- `BitcoinRpcPassword`
- `Network`
- `DatabasePath`

### 3) Arranque

```bash
dotnet run --project src/Electre
```

Por defecto escucha en:
- TCP: `50001`
- SSL: `50002` (si hay certificado configurado)

## Cómo puede evolucionar o escalar el diseño

### Escalado técnico

- **Separar planos de carga**: mantener un nodo para ingestión/sync y uno o más nodos read-heavy para servir clientes.
- **Particionado por red**: desplegar instancias independientes para `mainnet`, `testnet`, `signet` y `regtest`.
- **Sharding funcional progresivo**:
  - servicio de indexación,
  - servicio de API Electrum,
  - servicio de suscripciones/notificaciones.
- **Backpressure y límites adaptativos**: ajustar límites de suscripción, concurrencia RPC y colas de descarga según telemetría real.
- **Mejora de tolerancia a fallos**: reforzar estrategias de recovery de estado "dirty", snapshots y playbooks de reindexación.

### Escalado organizativo (equipo)

- **Contratos claros entre módulos**: definir interfaces estables entre `Protocol`, `Indexer`, `Database` y `Bitcoin` para reducir acoplamiento.
- **Gobierno de cambios**: PR templates, checklist técnico (perf, logging, errores, migraciones de config), revisión obligatoria para áreas sensibles.
- **Testing por capas**:
  - unit tests para componentes críticos,
  - tests de integración contra Bitcoin Core,
  - smoke tests de arranque/sync en CI.
- **Entornos reproducibles**: devcontainer o compose para levantar Core + Electre + métricas en local.
- **Roadmap de ownership**: repartir ownership por dominios (RPC, indexador, protocolo, observabilidad) y definir SLAs internos.

## Trade-offs asumidos

- Se prioriza **claridad y operabilidad** sobre microoptimizaciones prematuras.
- Se centraliza mucha lógica en pocos ficheros para velocidad de iteración inicial; a medida que crezca el equipo, conviene modularizar progresivamente.
- Se depende de Bitcoin Core para coherencia de consenso: menos flexibilidad, pero mayor fiabilidad de datos.

## Estado actual y siguientes pasos recomendados

- Consolidar una política de logs por entorno (dev/staging/prod).
- Añadir benchmarks de hot paths (mempool, UTXO lookups, histórico).
- Automatizar tests de integración en CI con fixtures de cadena pequeña.
- Definir objetivos SLO (latencia p95, tiempo de catch-up, error rate RPC).

## Licencia

MIT
