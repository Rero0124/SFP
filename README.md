# SFP (Segmented Forward Protocol)

A UDP-based **NACK-driven block-assembly** transmission protocol implemented in Rust.

> Designed for high-throughput file transfer in environments where TCP struggles — high latency, packet loss, low-spec receivers, and multi-path networks.

---

## ⚡ Performance

### Native (macOS, 10MB)
| Mode | Throughput | NACKs | Redundancy |
|------|-----------|-------|------------|
| Unencrypted | **~260 MB/s** | 0 | 5% (dynamic) |
| Encrypted (ChaCha20-Poly1305) | **~250 MB/s** | 0 | 5% (dynamic) |

### Virtualized (WSL2 on Windows, 10MB)
| Mode | Throughput | NACKs | Redundancy |
|------|-----------|-------|------------|
| Unencrypted | **~65 MB/s** | 0 | 5% (dynamic) |
| Encrypted (ChaCha20-Poly1305) | **~65 MB/s** | 0 | 5% (dynamic) |

> **Note:** WSL2 throughput is limited by per-packet syscall overhead (1472-byte UDP datagrams). Native OS avoids this overhead via `recvmmsg` batch receive.

### Cross-network (macOS ↔ WSL2 over WiFi, 10MB)
| Mode | Throughput | NACKs | RTT | Success |
|------|-----------|-------|-----|---------|
| Unencrypted | **~48 MB/s** | 0 | ~5ms | 100% |
| Encrypted (ChaCha20-Poly1305) | **~48 MB/s** | 0 | ~5ms | 100% |

### Simulated network conditions (Linux `tc netem` via network namespaces, 50MB)
| Condition | Throughput | NACKs | Success |
|-----------|-----------|-------|---------|
| 100ms RTT, 3% loss (encrypted) | **~1.9 MB/s** | 4,754 | 100% |
| 100ms RTT, 5% loss | **~1.9 MB/s** | 4,511 | 100% |
| 100ms RTT, 5% loss (realtime 15s) | **~2.0 MB/s** | 583 | 94.7% |

> **Note:** Loopback (`lo`) does not apply `tc netem` to UDP traffic — use network namespaces + veth pairs for accurate simulation. See [Network Simulation](#-network-simulation-tc-netem) below.

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
| ProbeUp | 1.25x | Periodic bandwidth re-probing |
| ProbeDrain | 0.75x | Queue drainage after probing |
| Cruise | 1.0x | Steady-state operation |

Key features:
- **Windowed max bandwidth**: Tracks maximum delivery rate over a 10-second sliding window (prevents EWMA death spiral)
- **Loss-based rate control**: Rate reduction under loss (1%/5%/10%/20%) + **rate boost when loss < 0.1%** (actively explores higher bandwidth)
- **Dynamic redundancy**: `recommended_redundancy()` reduces redundancy to minimum when loss-free, scales up under loss
- **Adaptive probing**: Cruise → ProbeUp interval shortens when loss is low (2s vs 5s), enabling faster bandwidth discovery
- **FlowControl feedback**: Client reports loss rate, processing rate, queue depth → server adjusts BBR

### 4. Dynamic Redundancy

BBR's `recommended_redundancy()` automatically scales redundancy based on loss:

```
loss < 0.1% → min_redundancy (bandwidth saved for speed)
loss 0.1~1% → gradual interpolation between min and base
loss 1~5%   → base + loss × 1.5 (gradual increase)
loss 5~20%  → base + loss × 2.5 (aggressive increase)
loss > 20%  → base + loss × 3.0 (maximum protection)
```

Example with 5% loss and base_redundancy=15%:
- `0.15 + 0.05 × 2.5 = 0.275` → 27.5% redundancy

Example on localhost (0% loss, min=5%, base=20%):
- Redundancy drops to 5%, saving 15% bandwidth for actual data

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

## 📊 Test Results

### Bulk Transfer (50MB, veth, 50ms RTT, 5% loss)
```
✅ 수신 완료!
   시간: 14.03s
   세그먼트: 800/800
   전송 성공률: 100.00%
   처리량: 3.56 MB/s
   NACK 전송 횟수: 1,007
   BBR: Cruise, redundancy 20%→26% (dynamic)
```

### Real-time Streaming (15s, veth, 50ms RTT, 5% loss)
```
🏁 실시간 스트리밍 완료
   프레임: 533 전송
   완료 확인: 506/533 (94.9%)
   평균 속도: 1.26 MB/s
   재전송: 1,644 청크
   BBR redundancy: 15%↔57% (dynamic)

🏁 실시간 수신 완료
   프레임 수신: 529
   지연 시간: avg 811ms, P50 650ms, P99 5939ms
```

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
