1 of the auditor becomes a coordinator.
Coordinator to all current storage nodes --> STOP!!!
Storage nodes stop sending acks to workers, but keep streaming entries to auditors.

Eventually workers stop committing.
Worker --> Coordinator: Send New Config (my RD Commit index)
Coordinator: Audits upto workers' rd commit index, then sends newest config as a reply.

Worker uses new storage servers to send entries.


