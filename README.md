# SFP (Segmented Forward Protocol)

A UDP-based **NACK-driven block-assembly** transmission protocol implemented in Rust.

> Designed for high-throughput file transfer in environments where TCP struggles — high latency, packet loss, low-spec receivers, and multi-path networks.

---

## ⚡ Performance

### Localhost (no network simulation)
| Mode | Throughput | NACKs |
|------|-----------|-------|
| Encrypted (X25519 + ChaCha20-Poly1305) | **~190 MB/s** sustained | ~52 |
| Unencrypted | ~250 MB/s peak | unstable (BBR tuning in progress) |

### Simulated network conditions (Linux `tc netem`: 100ms RTT, 3% packet loss)
| Mode | Throughput | NACKs |
|------|-----------|-------|
| Encrypted | **~68 MB/s** stable | 189 |

> **Note:** Direct TCP comparison on loopback is not meaningful due to kernel buffer optimization. TCP shows near-zero retransmissions even under simulated loss. SFP's advantage is designed for real WAN/lossy environments where TCP congestion window collapse is a bottleneck.

---

## 🔥 Core Design

- **NACK-only**: No ACK overhead — only missing chunks are requested
- **Block/puzzle assembly**: Segment-based (not stream-based) — each segment assembled independently
- **Forward Redundancy**: Proactive duplicate transmission to absorb packet loss without waiting for NACK
- **Low-spec optimized**: Minimal client-side processing burden
- **BBR-lite congestion control**: RTT/bandwidth-based dynamic pacing *(active development)*
- **Backpressure**: Queue-based automatic flow control

---

## 📦 Project Structure

```
SFP/
├── src/
│   ├── lib.rs           # Library entry point
│   ├── bbr.rs           # BBR-lite congestion control
│   ├── chunk.rs         # Segment/Chunk definitions
│   ├── config.rs        # Protocol configuration
│   ├── crypto.rs        # X25519 + ChaCha20-Poly1305 encryption
│   ├── error.rs         # Error types
│   ├── message.rs       # Protocol messages (NACK, etc.)
│   ├── multipath.rs     # Multi-path management
│   ├── receiver.rs      # Receiver (client)
│   ├── sender.rs        # Sender (server)
│   ├── stats.rs         # Transfer statistics
│   └── bin/
│       ├── server.rs    # Server binary
│       └── client.rs    # Client binary
├── examples/
│   └── large_file_test.rs  # 2GB file transfer benchmark
└── Cargo.toml
```

---

## 🚀 Build & Run

```bash
# Build
cargo build --release

# Server (sender)
cargo run --release --bin sfp-server -- --bind 0.0.0.0:9000 --file data.bin

# Client (receiver)
cargo run --release --bin sfp-client -- --server 127.0.0.1:9000 --output received.bin

# Benchmark: 2GB encrypted transfer
cargo run --release --example large_file_test -- --server --size 2000 --encrypt
cargo run --release --example large_file_test -- --client --encrypt
```

---

## 📊 Protocol Overview

### Transfer Units

| Unit | Size | Description |
|------|------|-------------|
| **Segment** | 64KB (default) | Logical block, assembly unit |
| **Chunk** | 1200 bytes (default) | UDP packet unit — the puzzle piece |

### Message Types

| Type | Direction | Description |
|------|-----------|-------------|
| `Init` | Client → Server | Connection init (public key, config negotiation) |
| `InitAck` | Server → Client | Init response (file size, segment count, session key) |
| `Chunk` | Server → Client | Data chunk |
| `NACK` | Client → Server | Request for missing chunks |
| `SegmentComplete` | Client → Server | Segment fully assembled |
| `FlowControl` | Client → Server | Flow control feedback (buffer, loss rate) |
| `Heartbeat` | Bidirectional | Keep-alive |
| `Close` | Bidirectional | Connection teardown |

### Transfer Flow

```
Server (Sender)                          Client (Receiver)
     │                                        │
     │<──────── Init ──────────────────────── │  ① Connect (public key, config)
     │──────── InitAck ──────────────────────>│  ② Response (file size, session key)
     │                                        │
     │  [Segment transmission loop 0..N]
     │                                        │
     │──── Chunk[seg_id, chunk_0] ───────────>│  ③ Chunks sent
     │──── Chunk[seg_id, chunk_N] ───────────>│
     │──── Redundant Chunk ──────────────────>│  ④ Forward Redundancy
     │                                        │
     │<──────── NACK [missing: 3,7,12] ───────│  ⑤ Request missing (only if needed)
     │──── Chunk[seg_id, chunk_3,7,12] ──────>│  ⑥ Retransmit from cache
     │                                        │
     │<──────── SegmentComplete ──────────────│  ⑦ Segment assembled
     │<──────── FlowControl ──────────────────│  ⑧ Periodic feedback
     │                                        │
     │<──────── Close ────────────────────────│  ⑨ Done
```

---

## 🔧 Configuration

```rust
use sfp::Config;

let config = Config::default();           // Balanced
let config = Config::low_spec();          // Low-spec devices
let config = Config::high_performance();  // High-throughput
let config = Config::unstable_network();  // High loss environments

// Custom
let mut config = Config::new();
config.chunk_size = 1200;
config.segment_size = 65536;
config.base_redundancy_ratio = 0.20; // 20% redundancy
```

### Forward Redundancy Ratio

| Network condition | Redundancy | Use case |
|-------------------|-----------|----------|
| Stable | 5–15% | Local / datacenter |
| Slightly lossy | 20–35% | General internet |
| Unstable | 40–60% | Mobile / satellite |
| Extreme loss | 70%+ | High-loss environments |

---

## 🔬 Key Components

### 1. NACK-based Block Transfer
No ACK → minimizes client uplink load. Only missing chunks are requested. Chunks are cached per-segment for fast retransmission without re-encryption.

### 2. X25519 + ChaCha20-Poly1305 Encryption
Key exchange via X25519 ECDH during handshake. Per-segment symmetric encryption with ChaCha20-Poly1305. Optional via `--encrypt` flag.

### 3. BBR-lite Congestion Control
RTT and delivery-rate based pacing. Currently tuned for stable environments — high-loss unencrypted mode shows instability due to buffer overflow when pacing rate exceeds receiver capacity. Active area of improvement.

### 4. Backpressure
Queue-capacity based flow control prevents sender from overwhelming the receiver:
- Pause threshold: < 70,000 queue slots
- Resume threshold: > 190,000 queue slots

### 5. FlowControl Feedback
Client periodically sends buffer availability, loss rate, and suggested rate back to the server for adaptive pacing.

---

## ⚠️ Known Limitations & Roadmap

- **BBR unstable without encryption**: In unencrypted mode, pacing rate overshoots receiver buffer capacity, causing NACK storms (~3800 NACKs vs ~52 in encrypted mode). Root cause identified — fix in progress.
- **SegmentComplete tracking**: Server-side segment completion counter shows 0 even after successful transfer. Client receives correctly; server-side state tracking bug under investigation.
- **Loopback only**: Not yet tested over real IP networks. WAN testing and NAT traversal are planned next steps.
- **BBR probe interval**: Currently fixed at 200ms. Adaptive probing under consideration.

---

## 🎯 SFP vs TCP/QUIC

| Environment | SFP | TCP | QUIC |
|-------------|-----|-----|------|
| Low-spec devices | ✅ Fast | ❌ ACK overhead | ⚠️ Complex |
| High-loss networks | ✅ Forward redundancy | ❌ Window collapse | ⚠️ RTT-dependent |
| Multi-path | ✅ Per-NIC ratio tuning | ❌ Not supported | ⚠️ Limited |
| High RTT (international) | ✅ RTT-independent | ❌ Severe degradation | ⚠️ Affected |

---

## 📁 Library Usage

### Server (Sender)

```rust
use sfp::{Config, Sender, PathManager};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let path_manager = Arc::new(PathManager::new(config.clone()));
    let sender = Sender::new(config, path_manager);
    sender.start("0.0.0.0:9000".parse()?).await?;
    Ok(())
}
```

### Client (Receiver)

```rust
use sfp::{Config, receiver::Receiver, PathManager};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::default();
    let path_manager = Arc::new(PathManager::new(config.clone()));

    let (receiver, mut segment_rx) = Receiver::start(
        config,
        "0.0.0.0:0".parse()?,
        "127.0.0.1:9000".parse()?,
        path_manager,
    ).await?;

    while let Some((segment_id, data)) = segment_rx.recv().await {
        println!("Received segment {}: {} bytes", segment_id, data.len());
    }
    Ok(())
}
```

---

## 📜 License

MIT License