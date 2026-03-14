# SFP (Segmented Forward Protocol)

A UDP-based **NACK-driven block-assembly** transmission protocol implemented in Rust.

> Designed for high-throughput file transfer in environments where TCP struggles — high latency, packet loss, low-spec receivers, and multi-path networks.

---

## ⚡ Performance

### Native (macOS, 4GB)
| Mode | Throughput | NACKs | Redundancy |
|------|-----------|-------|------------|
| Unencrypted | **~200 MB/s** | 0 | 5% (dynamic) |
| Encrypted (ChaCha20-Poly1305) | **~200 MB/s** | 0 | 5% (dynamic) |

### Cross-network (macOS ↔ macOS over WiFi, 2GB)
| Mode | Throughput | Network Bandwidth | NACKs | Redundancy |
|------|-----------|-------------------|-------|------------|
| Unencrypted | **~11 MB/s** | ~11 MB/s | 0 | 5% (dynamic) |
| Encrypted (ChaCha20-Poly1305) | **~12 MB/s** | ~12 MB/s | 0 | 5% (dynamic) |

> **Note:** Cross-network throughput matches iperf3-measured WiFi bandwidth — SFP achieves near-100% link utilization.
>
> **Note:** Native throughput is currently bottlenecked by BBR pacing (not yet fully optimized). Raw protocol capacity is significantly higher.

---

## 🔥 Core Design

- **NACK-only**: No ACK overhead — only missing chunks are requested
- **Block/puzzle assembly**: Segment-based (not stream-based) — each segment assembled independently
- **Forward Redundancy**: Proactive duplicate transmission to absorb packet loss without waiting for NACK
- **Dynamic Redundancy**: BBR loss detection automatically adjusts redundancy ratio (15% → 70% under high loss)
- **Low-spec optimized**: Minimal client-side processing burden
- **BBR-lite congestion control**: Windowed max bandwidth + gain cycling (Startup/ProbeUp/ProbeDrain/Cruise)
- **Backpressure**: Queue-based automatic flow control
- **Real-time streaming**: Continuous frame-based transmission with latency tracking

---

## 📦 Project Structure

```
SFP/
├── src/
│   ├── lib.rs           # Library entry point
│   ├── bbr.rs           # BBR-lite congestion control (windowed max BW + dynamic redundancy)
│   ├── chunk.rs         # Segment/Chunk definitions
│   ├── config.rs        # Protocol configuration
│   ├── crypto.rs        # X25519 + ChaCha20-Poly1305 encryption
│   ├── error.rs         # Error types
│   ├── message.rs       # Protocol messages (NACK, FlowControl, etc.)
│   ├── multipath.rs     # Multi-path management
│   ├── receiver.rs      # Receiver (client)
│   ├── sender.rs        # Sender (server)
│   ├── stats.rs         # Transfer statistics
│   └── bin/
│       ├── server.rs    # Server binary
│       └── client.rs    # Client binary
├── examples/
│   ├── large_file_test.rs  # Bulk file transfer benchmark (BBR + dynamic redundancy)
│   └── realtime_test.rs    # Real-time streaming test (latency tracking)
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
```

### Bulk File Transfer Test

```bash
# Terminal 1: Server (50MB test)
cargo run --release --example large_file_test -- --server --size 50

# Terminal 2: Client
cargo run --release --example large_file_test -- --client

# With encryption
cargo run --release --example large_file_test -- --server --size 2000 --encrypt
cargo run --release --example large_file_test -- --client --encrypt

# Remote server
cargo run --release --example large_file_test -- --server --size 100 --bind 0.0.0.0:9000
cargo run --release --example large_file_test -- --client --bind 192.168.1.100:9000
```

### Real-time Streaming Test

```bash
# Terminal 1: Server (30초간 10MB/s 스트리밍)
cargo run --release --example realtime_test -- --server --duration 30 --rate 10

# Terminal 2: Client
cargo run --release --example realtime_test -- --client

# Options
cargo run --release --example realtime_test -- --server --duration 60 --rate 20 --bind 0.0.0.0:9100
cargo run --release --example realtime_test -- --client --bind 192.168.1.100:9100
```

---

## 🌐 Network Simulation (tc netem)

Linux loopback (`lo`) bypasses TC qdisc for local UDP traffic. To simulate packet loss and delay, use **network namespaces + veth pairs**:

```bash
# Setup
sudo ip netns add ns_server
sudo ip netns add ns_client
sudo ip link add veth_s type veth peer name veth_c
sudo ip link set veth_s netns ns_server
sudo ip link set veth_c netns ns_client
sudo ip netns exec ns_server ip addr add 10.0.0.1/24 dev veth_s
sudo ip netns exec ns_server ip link set veth_s up
sudo ip netns exec ns_server ip link set lo up
sudo ip netns exec ns_client ip addr add 10.0.0.2/24 dev veth_c
sudo ip netns exec ns_client ip link set veth_c up
sudo ip netns exec ns_client ip link set lo up

# Apply netem (양방향 50ms delay, 5% loss)
sudo ip netns exec ns_server tc qdisc add dev veth_s root netem delay 50ms loss 5%
sudo ip netns exec ns_client tc qdisc add dev veth_c root netem delay 50ms loss 5%

# Run test
sudo ip netns exec ns_server cargo run --release --example large_file_test -- \
  --server --size 50 --bind 10.0.0.1:9000
sudo ip netns exec ns_client cargo run --release --example large_file_test -- \
  --client --bind 10.0.0.1:9000

# Real-time streaming test
sudo ip netns exec ns_server cargo run --release --example realtime_test -- \
  --server --bind 10.0.0.1:9100 --duration 15 --rate 5
sudo ip netns exec ns_client cargo run --release --example realtime_test -- \
  --client --bind 10.0.0.1:9100

# Cleanup
sudo ip netns del ns_server
sudo ip netns del ns_client
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
| `FlowControl` | Client → Server | Flow control feedback (loss rate, processing rate, queue depth) |
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
     │──── Chunk[seg_id, chunk_0] ───────────>│  ③ Chunks sent (BBR paced)
     │──── Chunk[seg_id, chunk_N] ───────────>│
     │──── Redundant Chunk ──────────────────>│  ④ Forward Redundancy (dynamic ratio)
     │                                        │
     │<──────── FlowControl ─────────────────│  ⑤ Loss rate + processing rate feedback
     │  [BBR adjusts pacing + redundancy]     │
     │                                        │
     │<──────── NACK [missing: 3,7,12] ───────│  ⑥ Request missing (only if needed)
     │──── Chunk[seg_id, chunk_3,7,12] ──────>│  ⑦ Retransmit from cache
     │                                        │
     │<──────── SegmentComplete ──────────────│  ⑧ Segment assembled
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
config.base_redundancy_ratio = 0.20; // 20% base redundancy
config.min_redundancy_ratio = 0.05;  // BBR will not go below 5%
config.max_redundancy_ratio = 0.70;  // BBR will not exceed 70%
```

### Forward Redundancy Ratio

| Network condition | Base Redundancy | BBR Dynamic Range | Use case |
|-------------------|----------------|-------------------|----------|
| Stable | 5–15% | 5–20% | Local / datacenter |
| Slightly lossy | 15–20% | 15–40% | General internet |
| Unstable | 20–35% | 20–60% | Mobile / satellite |
| Extreme loss | 35%+ | 35–80% | High-loss environments |

> BBR dynamically adjusts redundancy within the configured min/max range based on detected packet loss. When loss spikes, redundancy increases automatically to reduce NACK dependency.

---

## 🔬 Key Components

### 1. NACK-based Block Transfer
No ACK → minimizes client uplink load. Only missing chunks are requested. Chunks are cached per-segment for fast retransmission without re-encryption.

### 2. X25519 + ChaCha20-Poly1305 Encryption
Key exchange via X25519 ECDH during handshake. Per-segment symmetric encryption with ChaCha20-Poly1305. Optional via `--encrypt` flag.

### 3. BBR-lite Congestion Control

**State machine**: `Startup` → `Cruise` ↔ `ProbeUp` ↔ `ProbeDrain`

| State | Gain | Description |
|-------|------|-------------|
| Startup | 2.0x | Initial bandwidth discovery (max 2s) |
| ProbeUp | 1.5x | Aggressive bandwidth re-probing |
| ProbeDrain | 0.5x | Fast queue drainage after probing |
| Cruise | 1.0x | Steady-state operation |

Key features:
- **Windowed max bandwidth**: Tracks maximum delivery rate over a 2-second sliding window (fast adaptation to bandwidth changes)
- **Gain-based rate control**: Rate determined purely by `max_bw × gain` — loss is handled by dynamic redundancy, not rate reduction
- **Dynamic redundancy**: `recommended_redundancy()` reduces redundancy to minimum when loss-free, scales up under loss
- **Adaptive probing**: Cruise → ProbeUp interval shortens when loss is low (1s vs 3s), enabling faster bandwidth discovery
- **FlowControl feedback**: Client reports loss rate, processing rate, queue depth → server adjusts BBR

### 4. Dynamic Redundancy

BBR's `recommended_redundancy()` automatically scales redundancy based on loss (NACK handles the rest):

```
loss < 0.1% → min_redundancy (bandwidth saved for speed)
loss 0.1~5% → loss × 1.5 (proportional to loss rate)
loss 5~15%  → loss × 1.2 (NACK supplements recovery)
loss > 15%  → max_redundancy
```

Example with 5% loss (min=5%, max=70%):
- `0.05 × 1.2 = 0.06` → 6% redundancy (NACK recovers the rest)

Example on localhost (0% loss, min=5%):
- Redundancy stays at 5%, maximizing bandwidth for actual data

### 5. Backpressure
Queue-capacity based flow control prevents sender from overwhelming the receiver:
- Pause threshold: < 70,000 queue slots
- Resume threshold: > 190,000 queue slots

### 6. FlowControl Feedback
Client periodically sends loss rate, processing rate, and queue depth to the server. Loss calculation uses **stale segment detection** (300ms threshold) to avoid counting in-flight segments as lost.

### 7. Real-time Streaming
The `realtime_test` example demonstrates continuous frame-based transmission:
- Frame-level backpressure (max 50 in-flight frames)
- Per-frame latency measurement (P50/P99)
- Dynamic redundancy adjusts per-frame based on BBR loss detection
- End-to-end latency tracking via embedded timestamps

---

## 🎯 SFP vs TCP/QUIC

| Environment | SFP | TCP | QUIC |
|-------------|-----|-----|------|
| Low-spec devices | ✅ Fast | ❌ ACK overhead | ⚠️ Complex |
| High-loss networks | ✅ Forward redundancy + dynamic adjustment | ❌ Window collapse | ⚠️ RTT-dependent |
| Multi-path | ✅ Per-NIC ratio tuning | ❌ Not supported | ⚠️ Limited |
| High RTT (international) | ✅ RTT-independent | ❌ Severe degradation | ⚠️ Affected |
| Real-time streaming | ✅ Frame-level pacing + latency tracking | ❌ Head-of-line blocking | ⚠️ Partial |

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
