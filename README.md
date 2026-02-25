# Electre

Servidor Electrum (protocol 1.6) implementat en .NET 10, amb Bitcoin Core com a font de veritat i RocksDB com a magatzem d'indexació.

## Objectiu del projecte

Electre està pensat per donar resposta ràpida a consultes Electrum (historial, UTXO, mempool i capçaleres) mantenint un model operatiu simple: un únic servei, configuració explícita i components separats per responsabilitat.

## Decisions tècniques preses

- **.NET 10 + Generic Host**: permet un model de serveis en background robust (`Syncer`, `TcpServer`, mètriques, health checks) amb injecció de dependències estàndard.
- **RocksDB com a base d'indexació**: escollida per throughput d'escriptura i latència de lectura en càrrega alta; es treballa amb column families per separar dominis (headers, UTXO, historial, metadata, undo, etc.).
- **Bitcoin Core via RPC**: la cadena i la mempool es llegeixen des de Core per evitar divergències de consens i reduir complexitat de validació pròpia.
- **Sincronització orientada a I/O**: descàrrega paral·lela de blocs (`BlockDownloader`) i indexació seqüencial/consistent (`Syncer`) per combinar velocitat i correcció.
- **Cache local per hot paths**: cache d'UTXO i caches LRU per reduir consultes repetides i estabilitzar latència en càrrega real.
- **Observabilitat des del primer dia**: logging amb Serilog, endpoint de mètriques (Prometheus) i health checks per facilitar operació i diagnòstic.
- **Configuració centralitzada**: tota la parametrització important passa per `Config`/`appsettings.json` (RPC, límits, ports, ritmes de polling, tuning de DB).

## Arquitectura (resum)

- `Program.cs`: bootstrap, DI i registre de serveis.
- `Indexer/Syncer.cs`: bucle principal de sincronització i reorg.
- `Bitcoin/RpcClient.cs`: client RPC resilient cap a Bitcoin Core.
- `Database/Store.cs`: capa d'accés a RocksDB.
- `Database/UtxoCache.cs`: cache d'UTXO per camins crítics.
- `Protocol/Methods.cs`: handlers JSON-RPC Electrum.
- `Server/TcpServer.cs` i sessions: gestió de connexions client.

## Execució ràpida

### 1) Requisits

- .NET SDK 10
- Bitcoin Core amb RPC habilitat

### 2) Configuració

Edita `src/Electre/appsettings.json` (mínim):

- `BitcoinRpcUrl`
- `BitcoinRpcUser`
- `BitcoinRpcPassword`
- `Network`
- `DatabasePath`

### 3) Arrencada

```bash
dotnet run --project src/Electre
```

Per defecte escolta en:
- TCP: `50001`
- SSL: `50002` (si hi ha certificat configurat)

## Com pot evolucionar o escalar-se el disseny

### Escalat tècnic

- **Separar plans de càrrega**: mantenir un node per ingestió/sync i un o més nodes read-heavy per servir clients.
- **Particionament per xarxa**: desplegar instàncies independents per `mainnet`, `testnet`, `signet` i `regtest`.
- **Sharding funcional progressiu**:
  - servei d'indexació,
  - servei d'API Electrum,
  - servei de subscripcions/notificacions.
- **Backpressure i límits adaptatius**: ajustar límits de subscripció, concurrència RPC i cues de descàrrega segons telemetria real.
- **Millora de tolerància a fallades**: reforçar estratègies de recover de estat "dirty", snapshots i playbooks de reindexació.

### Escalat organitzatiu (equip)

- **Contractes clars entre mòduls**: definir interfícies estables entre `Protocol`, `Indexer`, `Database` i `Bitcoin` per reduir acoblament.
- **Govern de canvis**: PR templates, checklist tècnic (perf, logging, errors, migracions de config), revisió obligatòria per àrees sensibles.
- **Testing en capes**:
  - unit tests per components crítics,
  - tests d'integració contra Bitcoin Core,
  - smoke tests d'arrencada/sync en CI.
- **Entorns reproductibles**: devcontainer o compose per aixecar Core + Electre + mètriques en local.
- **Roadmap per ownership**: repartir ownership per dominis (RPC, indexador, protocol, observabilitat) i definir SLAs interns.

## Trade-offs assumits

- Es prioritza **claredat i operabilitat** sobre microoptimitzacions prematures.
- Es centralitza molta lògica en pocs fitxers per velocitat d'iteració inicial; a mesura que creixi l'equip, convé modularitzar progressivament.
- Es depèn de Bitcoin Core per coherència de consens: menys flexibilitat, però més fiabilitat de dades.

## Estat actual i següents passos recomanats

- Consolidar una política de logs per entorn (dev/staging/prod).
- Afegir benchmarks de hot paths (mempool, UTXO lookups, històric).
- Automatitzar tests d'integració en CI amb fixtures de cadena petita.
- Definir objectius SLO (latència p95, temps de catch-up, error rate RPC).

## Llicència

MIT
