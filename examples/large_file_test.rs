//! 대용량 파일 전송 테스트 (병렬 처리 + 암호화 지원)
//!
//! 사용법:
//!   cargo run --release --example large_file_test -- [OPTIONS]
//!
//! 옵션:
//!   --size <MB>       [서버 전용] 테스트 데이터 크기 (MB, 기본: 10)
//!   --server          서버 모드로 실행
//!   --client          클라이언트 모드로 실행 (서버와는 별개로 동작)
//!   --bind, -b <ADDR> 서버/클라이언트 주소 (기본: 127.0.0.1:9000)
//!   --encrypt, -e     암호화 활성화 (X25519 + ChaCha20-Poly1305)
//!   --workers <N>     [서버 전용] 병렬 워커 수 (기본: CPU 코어 수)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use sfp::bbr::BbrLite;
use sfp::chunk::SegmentBuilder;
use sfp::crypto::{CryptoSession, EphemeralKeyPair, KeyExchangeMessage};
use sfp::message::{FlowControlMessage, InitAckMessage, InitMessage, MessageHeader, MessageType, NackMessage, SegmentCompleteMessage};
use sfp::Config;

/// 테스트용 텍스트 데이터 생성
fn generate_test_text(size_mb: usize) -> Vec<u8> {
    let target_size = size_mb * 1024 * 1024;
    let mut data = Vec::with_capacity(target_size);

    // 다양한 텍스트 패턴 생성
    let patterns = [
        "The quick brown fox jumps over the lazy dog. ",
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ",
        "가나다라마바사아자차카타파하 ",
        "Hello, World! This is SFP Protocol test data. ",
        "🚀 UDP-based NACK block assembly protocol testing... ",
    ];

    let mut line_num = 0u64;
    while data.len() < target_size {
        // 줄 번호 추가
        let line = format!(
            "[{:08}] {}\n",
            line_num,
            patterns[line_num as usize % patterns.len()]
        );
        data.extend_from_slice(line.as_bytes());
        line_num += 1;
    }

    data.truncate(target_size);
    data
}

/// 데이터 검증 (첫 부분과 끝 부분 확인)
#[allow(dead_code)]
fn verify_data(original: &[u8], received: &[u8]) -> bool {
    if original.len() != received.len() {
        warn!(
            "크기 불일치: expected {} bytes, got {} bytes",
            original.len(),
            received.len()
        );
        return false;
    }

    // 전체 비교
    let mismatches: Vec<usize> = original
        .iter()
        .zip(received.iter())
        .enumerate()
        .filter(|(_, (a, b))| a != b)
        .map(|(i, _)| i)
        .take(10)
        .collect();

    if !mismatches.is_empty() {
        warn!("데이터 불일치 위치: {:?}", mismatches);
        return false;
    }

    true
}

/// 서버 (송신자) 실행 - 병렬 처리 + 암호화 지원
async fn run_server(
    addr: SocketAddr, 
    data: Vec<u8>, 
    config: Config,
    encrypt: bool,
    _num_workers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = Arc::new(UdpSocket::bind(addr).await?);
    info!("📡 서버 시작: {}", addr);
    info!("📦 전송 데이터: {} bytes ({:.2} MB)", data.len(), data.len() as f64 / 1024.0 / 1024.0);
    info!("⚙️  청크 크기: {} bytes", config.chunk_size);
    info!("⚙️  세그먼트 크기: {} bytes", config.segment_size);
    info!("⚙️  중복률: {:.1}%", config.base_redundancy_ratio * 100.0);
    info!("⚙️  암호화: {}", if encrypt { "✅ 활성화" } else { "❌ 비활성화" });

    // ═══════════════════════════════════════════════════════════════
    // 송신 큐: 우선순위 큐 (Init, InitAck, KeyExchange) + 데이터 큐 (청크)
    // ═══════════════════════════════════════════════════════════════
    let (priority_tx, mut priority_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1000);
    let (data_tx, mut data_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(200_000);

    // ─────────────────────────────────────────────────────────────────
    // 단일 송신 태스크 (개별 send_to)
    // ─────────────────────────────────────────────────────────────────
    let send_socket = socket.clone();

    let _send_task = tokio::spawn(async move {
        loop {
            // 우선순위 큐 먼저 체크
            match priority_rx.try_recv() {
                Ok((bytes, addr)) => {
                    let _ = send_socket.send_to(&bytes, addr).await;
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {}
            }

            tokio::select! {
                biased;
                Some((bytes, addr)) = priority_rx.recv() => {
                    let _ = send_socket.send_to(&bytes, addr).await;
                }
                Some((bytes, addr)) = data_rx.recv() => {
                    let _ = send_socket.send_to(&bytes, addr).await;
                }
                else => break,
            }
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // 수신 큐 + 수신 태스크 (모든 수신은 이 큐를 통해)
    // ═══════════════════════════════════════════════════════════════
    let (recv_tx, recv_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(100_000);
    let recv_rx = Arc::new(tokio::sync::Mutex::new(recv_rx));
    
    let recv_socket = socket.clone();
    let recv_drop_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let recv_drop_log = recv_drop_count.clone();
    let _recv_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        let mut err_count = 0u64;
        loop {
            match recv_socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    if recv_tx.try_send((buf[..len].to_vec(), addr)).is_err() {
                        recv_drop_log.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    err_count += 1;
                    if err_count % 1000 == 1 {
                        eprintln!("⚠️ recv_task 에러 #{}", err_count);
                    }
                    continue;
                }
            }
        }
    });

    info!("⏳ 클라이언트 연결 대기 중...");

    // Init 메시지 대기 (수신 큐에서)
    let (client_addr, crypto_session, client_timestamp) = loop {
        let mut rx = recv_rx.lock().await;
        if let Some((data, addr)) = rx.recv().await {
            drop(rx);
            
            if let Ok(header) = bincode::deserialize::<MessageHeader>(&data[..data.len().min(32)]) {
                if header.msg_type == MessageType::Init {
                    info!("✅ 클라이언트 연결: {}", addr);
                    
                    // Init 메시지 파싱하여 타임스탬프 추출
                    let init_timestamp = InitMessage::from_bytes(&data)
                        .map(|m| m.timestamp_us)
                        .unwrap_or(0);

                    // 암호화 설정 (키 교환)
                    let crypto = if encrypt {
                        info!("🔐 키 교환 시작...");
                        
                        let server_keypair = EphemeralKeyPair::generate();
                        let server_public = server_keypair.public_key_bytes();
                        let key_msg = KeyExchangeMessage { public_key: server_public };
                        
                        // 클라이언트 공개키 수신 (수신 큐에서)
                        let client_key_msg = loop {
                            let _ = priority_tx.send((key_msg.to_bytes(), addr)).await;
                            
                            let mut rx = recv_rx.lock().await;
                            match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
                                Ok(Some((data, _))) => {
                                    drop(rx);
                                    if let Some(msg) = KeyExchangeMessage::from_bytes(&data) {
                                        break msg;
                                    }
                                }
                                Ok(None) => return Err("수신 채널 종료".into()),
                                Err(_) => {
                                    drop(rx);
                                    info!("🔐 키 교환 재전송...");
                                }
                            }
                        };
                        
                        let session = CryptoSession::establish(server_keypair, client_key_msg.public_key);
                        info!("🔐 키 교환 완료!");
                        
                        Some(Arc::new(Mutex::new(session)))
                    } else {
                        None
                    };

                    break (addr, crypto, init_timestamp);
                }
            }
        }
    };

    // InitAck 전송 (클라이언트 타임스탬프 에코 - RTT 측정용)
    let ack = InitAckMessage::with_client_timestamp(
        data.len() as u64,
        config.chunk_size as u16,
        config.segment_size as u32,
        config.base_redundancy_ratio as f32,
        client_timestamp,
    );
    let _ = priority_tx.send((ack.to_bytes(), client_addr)).await;

    // 세그먼트 준비 (병렬 처리)
    let segment_builder = Arc::new(SegmentBuilder::new(config.chunk_size));
    let data = Arc::new(data);
    let total_segments = (data.len() + config.segment_size - 1) / config.segment_size;
    
    info!("🚀 전송 시작: {} 세그먼트", total_segments);

    // 세그먼트별 청크 저장 (재전송용)
    let segment_chunks: Arc<RwLock<HashMap<u64, Vec<sfp::chunk::Chunk>>>> = 
        Arc::new(RwLock::new(HashMap::new()));

    // BBR 혼잡 제어
    let initial_rtt = 0.001; // 1ms
    let initial_rate = 300_000_000.0; // 300 MB/s
    let _packet_size = config.chunk_size + 100;
    let bbr = Arc::new(tokio::sync::Mutex::new(BbrLite::new(initial_rtt, initial_rate)));
    let segments_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sending_active = Arc::new(std::sync::atomic::AtomicBool::new(true));

    // NACK 처리 준비 (sending 중에도 수신 메시지를 처리하기 위해 미리 시작)
    let retransmit_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let last_nack_time = Arc::new(tokio::sync::RwLock::new(Instant::now()));
    let completed_segments: Arc<tokio::sync::RwLock<std::collections::HashSet<u64>>> =
        Arc::new(tokio::sync::RwLock::new(std::collections::HashSet::new()));
    let nack_running = Arc::new(std::sync::atomic::AtomicBool::new(true));

    let (nack_tx, nack_rx) = mpsc::channel::<NackMessage>(10000);
    let nack_rx = Arc::new(tokio::sync::Mutex::new(nack_rx));

    // 통합 디스패처 (Init, SegmentComplete, NACK, FlowControl 모두 처리)
    // sending 중에도 실행하여 메시지 유실 방지
    let disp_running = nack_running.clone();
    let disp_last_nack = last_nack_time.clone();
    let disp_completed = completed_segments.clone();
    let ack_bytes = ack.to_bytes();
    let priority_tx_disp = priority_tx.clone();
    let recv_rx_disp = recv_rx.clone();
    let disp_bbr = bbr.clone();
    let disp_base_redundancy = config.base_redundancy_ratio;
    let disp_min_redundancy = config.min_redundancy_ratio;
    let disp_max_redundancy = config.max_redundancy_ratio;

    let dispatcher_task = tokio::spawn(async move {
        let mut last_bbr_log = Instant::now();

        while disp_running.load(std::sync::atomic::Ordering::Relaxed) {
            let mut rx = recv_rx_disp.lock().await;
            match tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
                Ok(Some((data, addr))) => {
                    drop(rx);

                    // 메시지 타입별 처리
                    if let Ok(header) = bincode::deserialize::<MessageHeader>(&data[..data.len().min(32)]) {
                        match header.msg_type {
                            MessageType::Init => {
                                let _ = priority_tx_disp.try_send((ack_bytes.clone(), addr));
                                continue;
                            }
                            MessageType::SegmentComplete => {
                                if let Some(msg) = sfp::message::SegmentCompleteMessage::from_bytes(&data) {
                                    disp_completed.write().await.insert(msg.segment_id);
                                }
                                continue;
                            }
                            _ => {}
                        }
                    }

                    // FlowControl 처리 → BBR 업데이트
                    if let Some(fc) = FlowControlMessage::from_bytes(&data) {
                        let mut b = disp_bbr.lock().await;
                        b.on_loss_update(fc.loss_rate as f64);
                        if fc.processing_rate > 0.0 {
                            b.on_receiver_rate_update(fc.processing_rate as f64);
                        }
                        if fc.segments_in_progress > 0 {
                            let queue_delay = fc.segments_in_progress as f64 * 0.0005;
                            b.on_rtt_update(0.001 + queue_delay);
                        } else {
                            b.on_rtt_update(0.001);
                        }
                        b.update_rate();
                        if last_bbr_log.elapsed() > Duration::from_millis(500) {
                            let dyn_redundancy = b.recommended_redundancy(
                                disp_base_redundancy, disp_min_redundancy, disp_max_redundancy);
                            info!("📶 BBR state:{:?} rate:{:.0}MB/s bw:{:.0}MB/s rtt:{:.2}ms loss:{:.1}% redundancy:{:.0}%",
                                b.state, b.pacing_rate / 1024.0 / 1024.0,
                                b.max_bw / 1024.0 / 1024.0,
                                b.min_rtt * 1000.0, b.loss_rate * 100.0,
                                dyn_redundancy * 100.0);
                            last_bbr_log = Instant::now();
                        }
                        continue;
                    }

                    // NACK 처리
                    if let Some(nack) = NackMessage::from_bytes(&data) {
                        *disp_last_nack.write().await = Instant::now();
                        let _ = nack_tx.try_send(nack);
                    }
                }
                Ok(None) => break,
                Err(_) => { drop(rx); continue; }
            }
        }
    });

    // NACK 재전송 workers
    let send_count = retransmit_count.clone();
    let num_process_workers = 4;
    let mut process_handles = Vec::new();
    let nack_bbr = bbr.clone();

    for _worker_id in 0..num_process_workers {
        let rx = nack_rx.clone();
        let chunks_cache = segment_chunks.clone();
        let tx = data_tx.clone();
        let worker_running = nack_running.clone();
        let send_counter = send_count.clone();
        let b = nack_bbr.clone();

        let handle = tokio::spawn(async move {
            loop {
                let nack = {
                    let mut rx_guard = rx.lock().await;
                    match tokio::time::timeout(Duration::from_millis(50), rx_guard.recv()).await {
                        Ok(Some(nack)) => nack,
                        Ok(None) => break,
                        Err(_) => {
                            if !worker_running.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            }
                            continue;
                        }
                    }
                };

                let cache = chunks_cache.read().await;
                if let Some(chunks) = cache.get(&nack.segment_id) {
                    for &chunk_id in &nack.missing_chunk_ids {
                        if let Some(chunk) = chunks.get(chunk_id as usize) {
                            let bytes = chunk.to_bytes();
                            let byte_len = bytes.len();
                            let _ = tx.send((bytes, client_addr)).await;
                            send_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            let mut guard = b.lock().await;
                            guard.on_packet_sent(byte_len);
                            guard.update_rate();
                            let budget = guard.bytes_per_interval(Duration::from_millis(10));
                            drop(guard);
                            if byte_len >= budget {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        }
                    }
                }
            }
        });
        process_handles.push(handle);
    }

    // 데이터 전송 (pacing 적용)
    let tx = data_tx.clone();
    let start = Instant::now();
    let mut total_chunks = 0u64;
    let mut total_redundant = 0u64;

    let segment_size = config.segment_size;
    let base_redundancy = config.base_redundancy_ratio;
    let min_redundancy = config.min_redundancy_ratio;
    let max_redundancy = config.max_redundancy_ratio;

    for segment_id in 1..=total_segments as u64 {
        let offset = (segment_id as usize - 1) * segment_size;
        let end = (offset + segment_size).min(data.len());
        let segment_data = &data[offset..end];

        let processed_data = if let Some(ref session) = crypto_session {
            let mut session = session.lock().await;
            session.encrypt(segment_id, segment_data)?
        } else {
            segment_data.to_vec()
        };

        // BBR 손실률 기반 동적 redundancy 조절
        let redundancy_ratio = {
            let b = bbr.lock().await;
            b.recommended_redundancy(base_redundancy, min_redundancy, max_redundancy)
        };

        let chunks = segment_builder.split_into_chunks(segment_id, &processed_data, 0);
        let redundant_chunks = segment_builder.create_redundant_chunks(&chunks, redundancy_ratio);

        {
            let mut cache = segment_chunks.write().await;
            cache.insert(segment_id, chunks.clone());
        }

        // 청크 전송 (채널에 적재) - 백프레셔 + interval 기반 BBR pacing
        const MIN_CAPACITY: usize = 70_000;
        const RESUME_CAPACITY: usize = 190_000;

        // 큐가 너무 차면 대기 (남은 용량이 적으면)
        while tx.capacity() < MIN_CAPACITY {
            tokio::time::sleep(Duration::from_micros(100)).await;
            if tx.capacity() >= RESUME_CAPACITY {
                break;
            }
        }

        // interval 기반 pacing
        const PACING_INTERVAL: Duration = Duration::from_millis(50);

        // 모든 청크(원본 + redundant) 합치기
        let all_chunks: Vec<_> = chunks.iter().chain(redundant_chunks.iter()).collect();
        let mut chunk_idx = 0;
        let mut interval_start = Instant::now();
        let mut interval_bytes_sent = 0usize;

        // 이 interval 동안 보낼 수 있는 바이트 수 계산
        let mut bytes_budget = {
            let b = bbr.lock().await;
            b.bytes_per_interval(PACING_INTERVAL)
        };

        while chunk_idx < all_chunks.len() {
            let chunk = all_chunks[chunk_idx];
            let bytes = chunk.to_bytes();
            let byte_len = bytes.len();
            let _ = tx.send((bytes, client_addr)).await;

            if chunk_idx < chunks.len() {
                total_chunks += 1;
            } else {
                total_redundant += 1;
            }
            interval_bytes_sent += byte_len;
            chunk_idx += 1;

            // interval budget 소진 시 → 남은 시간 sleep 후 다음 interval
            if interval_bytes_sent >= bytes_budget {
                {
                    let mut b = bbr.lock().await;
                    b.on_packet_sent(interval_bytes_sent);
                    b.update_rate();
                    bytes_budget = b.bytes_per_interval(PACING_INTERVAL);
                }
                let elapsed = interval_start.elapsed();
                if elapsed < PACING_INTERVAL {
                    tokio::time::sleep(PACING_INTERVAL - elapsed).await;
                }
                interval_start = Instant::now();
                interval_bytes_sent = 0;
            }
        }

        // 잔여 바이트 기록
        if interval_bytes_sent > 0 {
            let mut b = bbr.lock().await;
            b.on_packet_sent(interval_bytes_sent);
        }

        segments_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if segment_id % 100 == 0 || segment_id == total_segments as u64 {
            let progress = (segment_id as f64 / total_segments as f64) * 100.0;
            let elapsed = start.elapsed().as_secs_f64();
            let speed = end as f64 / elapsed / 1024.0 / 1024.0;
            let b = bbr.lock().await;
            info!("📊 진행: {:.1}% | {}/{} | {:.0} MB/s | state:{:?} rate:{:.0}MB/s redundancy:{:.0}%",
                progress, segment_id, total_segments, speed, b.state, b.pacing_rate / 1024.0 / 1024.0,
                redundancy_ratio * 100.0);
        }
    }

    // 전송 완료
    sending_active.store(false, std::sync::atomic::Ordering::Relaxed);
    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let elapsed = start.elapsed();
    let throughput = data.len() as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("✅ 1차 전송 완료!");
    info!("   시간: {:.2}s", elapsed.as_secs_f64());
    info!("   총 청크: {} (원본) + {} (중복)", total_chunks, total_redundant);
    info!("   처리량: {:.2} MB/s", throughput);
    if encrypt {
        info!("   암호화: ChaCha20-Poly1305");
    }
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // NACK 대기
    let nack_wait_secs = ((data.len() as u64 / (1 * 1024 * 1024)) + 120).max(300);
    info!("⏳ NACK 대기 및 재전송 중 (최대 {}초)...", nack_wait_secs);

    // last_nack_time 리셋 (sending 완료 시점 기준으로)
    *last_nack_time.write().await = Instant::now();

    // 모니터링 루프
    let nack_start = Instant::now();
    let mut last_log_time = Instant::now();
    
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let last_nack = *last_nack_time.read().await;
        let completed_count = completed_segments.read().await.len();
        let retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);
        
        if last_log_time.elapsed() > Duration::from_secs(2) && retrans > 0 {
            let drops = recv_drop_count.load(std::sync::atomic::Ordering::Relaxed);
            info!("📨 재전송 진행: {} 청크 | 완료: {}/{} | recv_drops: {}", retrans, completed_count, total_segments, drops);
            last_log_time = Instant::now();
        }
        
        if last_nack.elapsed() > Duration::from_secs(30) && retrans > 0 {
            info!("⏱️  30초간 NACK 없음, 전송 완료로 간주");
            break;
        }
        
        if completed_count >= total_segments {
            info!("✅ 모든 세그먼트 완료 확인!");
            break;
        }
        
        if nack_start.elapsed() > Duration::from_secs(nack_wait_secs) {
            info!("⏱️  NACK 대기 시간 초과");
            break;
        }
    }
    
    nack_running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = dispatcher_task.await;
    for handle in process_handles {
        let _ = handle.await;
    }
    
    let final_retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_completed = completed_segments.read().await.len();

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("🏁 서버 종료");
    info!("   총 재전송: {} 청크", final_retrans);
    info!("   완료 세그먼트: {}/{}", final_completed, total_segments);
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    Ok(())
}

/// 클라이언트 (수신자) 실행 - 병렬 처리 + 암호화 지원
/// 
/// 서버 주소만 지정하면 나머지 설정은 InitAck에서 수신
async fn run_client(
    server_addr: SocketAddr,
    encrypt: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    info!("📡 클라이언트 시작");
    info!("🎯 서버: {}", server_addr);
    info!("⚙️  암호화: {}", if encrypt { "✅ 활성화" } else { "❌ 비활성화" });

    // 수신용 블로킹 소켓
    let recv_std_socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    recv_std_socket.set_read_timeout(Some(Duration::from_millis(500)))?;
    info!("📌 수신 소켓: {}", recv_std_socket.local_addr()?);
    // 대용량 수신 버퍼 설정 (socket2 경유)
    {
        let s2 = socket2::SockRef::from(&recv_std_socket);
        let _ = s2.set_recv_buffer_size(64 * 1024 * 1024);
    }

    // 송신용 별도 tokio 소켓 (O_NONBLOCK 공유 문제 회피)
    let send_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);

    // ═══════════════════════════════════════════════════════════════
    // 단일 송신 큐 + 송신 태스크 (모든 전송은 이 큐를 통해)
    // ═══════════════════════════════════════════════════════════════
    let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(1000);

    let send_socket_clone = send_socket.clone();
    let _send_task = tokio::spawn(async move {
        while let Some(bytes) = send_rx.recv().await {
            let _ = send_socket_clone.send_to(&bytes, server_addr).await;
        }
    });

    // ═══════════════════════════════════════════════════════════════
    // Init/InitAck 핸드쉐이크 (블로킹 std 소켓으로 직접 송수신)
    // tokio 런타임을 블로킹하지 않도록 spawn_blocking 사용
    // ═══════════════════════════════════════════════════════════════
    let init_msg = sfp::message::InitMessage::new(encrypt, [0u8; 32]);
    let handshake_encrypt = encrypt;
    let handshake_server_addr = server_addr;

    let (init_ack, crypto_session, rtt_us, recv_std_socket) = tokio::task::spawn_blocking(move || -> Result<(sfp::message::InitAckMessage, Option<Arc<tokio::sync::Mutex<CryptoSession>>>, u64, std::net::UdpSocket), String> {
        let init_send_time = Instant::now();
        let mut last_init_send = Instant::now();

        info!("📤 Init 전송 (서버 응답 대기 중)...");
        let _ = recv_std_socket.send_to(&init_msg.to_bytes(), handshake_server_addr);

        loop {
            // 10초 타임아웃
            if init_send_time.elapsed() > Duration::from_secs(10) {
                return Err("서버 응답 타임아웃 (10초) - 서버가 실행 중인지 확인하세요".into());
            }

            let mut buf = [0u8; 2048];
            match recv_std_socket.recv_from(&mut buf) {
                Ok((len, _src)) => {
                    let data = &buf[..len];

                    // InitAck 체크
                    if let Some(ack) = sfp::message::InitAckMessage::from_bytes(data) {
                        let rtt = init_send_time.elapsed().as_micros() as u64;
                        info!("✅ InitAck 수신 (RTT: {}μs)", rtt);
                        return Ok((ack, None, rtt, recv_std_socket));
                    }

                    // 암호화 키 교환
                    if handshake_encrypt {
                        if let Some(server_key_msg) = KeyExchangeMessage::from_bytes(data) {
                            info!("🔑 서버 공개키 수신 완료");
                            let client_keypair = EphemeralKeyPair::generate();
                            let client_public = client_keypair.public_key_bytes();
                            let key_msg = KeyExchangeMessage { public_key: client_public };
                            let _ = recv_std_socket.send_to(&key_msg.to_bytes(), handshake_server_addr);
                            info!("🔑 클라이언트 공개키 전송 완료");

                            let session = CryptoSession::establish(client_keypair, server_key_msg.public_key);
                            info!("🔐 키 교환 완료!");
                            let crypto = Some(Arc::new(tokio::sync::Mutex::new(session)));

                            let ack = loop {
                                let mut buf2 = [0u8; 2048];
                                match recv_std_socket.recv_from(&mut buf2) {
                                    Ok((len, _)) => {
                                        if let Some(ack) = sfp::message::InitAckMessage::from_bytes(&buf2[..len]) {
                                            break ack;
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                                        || e.kind() == std::io::ErrorKind::TimedOut => {
                                        let _ = recv_std_socket.send_to(&init_msg.to_bytes(), handshake_server_addr);
                                    }
                                    Err(_) => return Err("수신 소켓 에러".into()),
                                }
                            };
                            let rtt = init_send_time.elapsed().as_micros() as u64;
                            return Ok((ack, crypto, rtt, recv_std_socket));
                        }
                    }

                    // 데이터 청크 등 다른 패킷은 무시 → 계속 수신
                    continue;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {
                    // 타임아웃 시 Init 재전송 (1초 간격)
                    if last_init_send.elapsed() > Duration::from_secs(1) {
                        let _ = recv_std_socket.send_to(&init_msg.to_bytes(), handshake_server_addr);
                        last_init_send = Instant::now();
                        info!("📤 Init 재전송 ({:.1}초 경과)...", init_send_time.elapsed().as_secs_f32());
                    }
                }
                Err(e) => {
                    return Err(format!("수신 소켓 에러: {}", e));
                }
            }
        }
    }).await.map_err(|e| e.to_string())?.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    
    info!("✅ InitAck 수신 완료");
    
    // RTT 기반 초기 대역폭 추정
    // InitAck 메시지 크기 ~200 bytes, RTT로 나누어 대역폭 추정
    // 로컬 환경에서 RTT가 매우 짧으면 높은 대역폭 추정
    let estimated_bandwidth_mbps = if rtt_us > 0 {
        // InitAck ~200 bytes * 2 (왕복) / RTT = bytes/sec
        // 실제 대역폭은 이보다 훨씬 높으므로 곱하기 1000
        let bw = (400.0 * 1_000_000.0 / rtt_us as f64) * 1000.0;
        (bw / 1024.0 / 1024.0).min(500.0).max(50.0) // 50~500 MB/s 범위
    } else {
        100.0 // 기본값 100 MB/s
    };
    info!("📊 RTT: {}μs → 추정 대역폭: {:.0} MB/s", rtt_us, estimated_bandwidth_mbps);
    
    // 초기 FlowControl 전송 — 아직 실측 데이터 없으므로 rate=0 전송
    // rate=0이면 서버 BBR이 initial_rate를 유지함 (피드백 없음으로 처리)
    let initial_fc = FlowControlMessage::new(1000, 0, 0, 0.0, 0.0);
    let _ = send_tx.send(initial_fc.to_bytes()).await;
    
    // 서버에서 받은 설정 정보
    let total_file_size = init_ack.total_file_size as usize;
    let expected_segments = init_ack.total_segments as usize;
    let segment_size = init_ack.segment_size as usize;
    let chunk_size = init_ack.chunk_size as usize;
    let chunks_per_segment = init_ack.chunks_per_segment as usize;
    
    info!("✅ InitAck 수신 완료:");
    info!("   파일 크기: {} bytes ({:.2} MB)", total_file_size, total_file_size as f64 / 1024.0 / 1024.0);
    info!("   총 세그먼트: {}", expected_segments);
    info!("   세그먼트 크기: {} bytes", segment_size);
    info!("   청크 크기: {} bytes", chunk_size);
    info!("   세그먼트당 청크: {}", chunks_per_segment);
    
    info!("✅ 서버 연결 완료, 데이터 수신 시작...");

    let start = Instant::now();

    // ═══════════════════════════════════════════════════════════════
    // std::thread 인라인 수신+처리 (tokio 오버헤드 제거, Vec 할당 제거)
    // recv_std_socket은 블로킹 모드로 std::thread에서 직접 사용
    // ═══════════════════════════════════════════════════════════════

    // 공유 상태 (atomic으로 모니터링)
    let total_chunks_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let assembled_count_atomic = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let last_chunk_time = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let raw_bytes_received = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // 완료된 세그먼트 데이터를 메인으로 전달
    let (seg_tx, seg_rx) = crossbeam_channel::bounded::<(u64, Vec<u8>)>(1000);
    // NACK/FC용 세그먼트 상태 스냅샷
    let segment_status_snapshot: Arc<parking_lot::RwLock<HashMap<u64, (Vec<bool>, u32)>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let segment_last_chunk_time: Arc<parking_lot::RwLock<HashMap<u64, Instant>>> =
        Arc::new(parking_lot::RwLock::new(HashMap::new()));

    // 완료된 세그먼트 ID 추적 (recv 스레드 ↔ 메인 루프 공유)
    let assembled_segments: Arc<parking_lot::RwLock<std::collections::HashSet<u64>>> =
        Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new()));

    // SegmentComplete 전송용 crossbeam
    let (complete_tx, complete_rx) = crossbeam_channel::bounded::<Vec<u8>>(1000);

    // recv_std_socket 타임아웃 설정 (50ms)
    recv_std_socket.set_read_timeout(Some(Duration::from_millis(50)))?;

    let w_chunks = total_chunks_received.clone();
    let w_assembled = assembled_count_atomic.clone();
    let w_last_chunk = last_chunk_time.clone();
    let w_running = running.clone();
    let w_seg_status = segment_status_snapshot.clone();
    let w_seg_last_time = segment_last_chunk_time.clone();
    let w_start = start;
    let w_raw_bytes = raw_bytes_received.clone();
    let w_assembled_segs = assembled_segments.clone();

    let recv_thread = std::thread::spawn(move || {

        struct SegmentBuffer {
            data: Vec<u8>,
            received: Vec<bool>,
            received_count: u32,
            total_chunks: u32,
            chunk_size: usize,
        }
        let mut segment_buffers: HashMap<u64, SegmentBuffer> = HashMap::new();
        let mut local_assembled: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut local_seg_times: HashMap<u64, Instant> = HashMap::new();
        let mut last_snapshot = Instant::now();

        // 배치 수신 크기
        const BATCH_SIZE: usize = 64;

        // 소켓을 non-blocking으로 설정
        recv_std_socket.set_nonblocking(true).expect("set_nonblocking failed");

        // 버퍼 할당 (BATCH_SIZE × 2048 bytes)
        let mut bufs = vec![[0u8; 2048]; BATCH_SIZE];
        let mut recv_lens = vec![0usize; BATCH_SIZE];

        loop {
            if !w_running.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            // non-blocking recv 루프로 배치 수신 (크로스 플랫폼)
            let mut n = 0usize;
            for i in 0..BATCH_SIZE {
                match recv_std_socket.recv(&mut bufs[i]) {
                    Ok(len) => {
                        recv_lens[i] = len;
                        n += 1;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            if n == 0 {
                // 수신할 데이터 없음 — 잠시 대기 후 재시도
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }

            let elapsed_us = w_start.elapsed().as_micros() as u64;
            w_last_chunk.store(elapsed_us, std::sync::atomic::Ordering::Relaxed);

            // raw 수신 바이트 카운트 (파싱 전, 순수 소켓 수신량)
            let mut batch_bytes = 0u64;
            for i in 0..n {
                batch_bytes += recv_lens[i] as u64;
            }
            w_raw_bytes.fetch_add(batch_bytes, std::sync::atomic::Ordering::Relaxed);

            // 배치 내 각 패킷 처리
            for i in 0..n {
                let len = recv_lens[i];
                let data = &bufs[i][..len];

                // 수동 헤더 파싱
                if data.len() < 2 + 16 { continue; }
                let header_len = u16::from_le_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + header_len { continue; }
                let h = &data[2..];
                let segment_id = u64::from_le_bytes([h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]]);
                let chunk_id = u32::from_le_bytes([h[8], h[9], h[10], h[11]]) as usize;
                let total_chunks = u32::from_le_bytes([h[12], h[13], h[14], h[15]]) as usize;
                let chunk_data = &data[2 + header_len..];

                if local_assembled.contains(&segment_id) { continue; }

                let sbuf = segment_buffers.entry(segment_id).or_insert_with(|| {
                    SegmentBuffer {
                        data: vec![0u8; total_chunks * chunk_size],
                        received: vec![false; total_chunks],
                        received_count: 0,
                        total_chunks: total_chunks as u32,
                        chunk_size,
                    }
                });

                if chunk_id < sbuf.received.len() && !sbuf.received[chunk_id] {
                    let offset = chunk_id * sbuf.chunk_size;
                    let end = (offset + chunk_data.len()).min(sbuf.data.len());
                    sbuf.data[offset..end].copy_from_slice(&chunk_data[..end - offset]);
                    sbuf.received[chunk_id] = true;
                    sbuf.received_count += 1;
                    w_chunks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    local_seg_times.insert(segment_id, Instant::now());
                }

                if sbuf.received_count >= sbuf.total_chunks {
                    let segment_data = std::mem::take(&mut sbuf.data);
                    local_assembled.insert(segment_id);
                    segment_buffers.remove(&segment_id);
                    w_assembled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    w_assembled_segs.write().insert(segment_id);
                    let _ = seg_tx.try_send((segment_id, segment_data));

                    let elapsed_ms = w_start.elapsed().as_millis() as u64;
                    let complete_msg = SegmentCompleteMessage::new(segment_id, total_chunks as u32, elapsed_ms);
                    let _ = complete_tx.try_send(complete_msg.to_bytes());
                }
            }

            // 주기적 스냅샷 (500ms마다) - parking_lot RwLock 사용
            if last_snapshot.elapsed() > Duration::from_millis(500) {
                {
                    let mut status = w_seg_status.write();
                    status.clear();
                    for (seg_id, sbuf) in &segment_buffers {
                        status.insert(*seg_id, (sbuf.received.clone(), sbuf.total_chunks));
                    }
                }
                {
                    let mut times = w_seg_last_time.write();
                    *times = local_seg_times.clone();
                }
                last_snapshot = Instant::now();
            }
        }
    });

    // ─────────────────────────────────────────────────────────────────
    // SegmentComplete 전달 태스크 (crossbeam → tokio send)
    // ─────────────────────────────────────────────────────────────────
    let complete_send_tx = send_tx.clone();
    let _complete_task = tokio::spawn(async move {
        loop {
            match complete_rx.try_recv() {
                Ok(bytes) => { let _ = complete_send_tx.try_send(bytes); }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => break,
            }
        }
    });

    // ─────────────────────────────────────────────────────────────────
    // 최종 결과 저장소
    // ─────────────────────────────────────────────────────────────────
    let decrypted_segments: Arc<Mutex<HashMap<u64, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let decrypt_segments = decrypted_segments.clone();
    let crypto = crypto_session.clone();
    let assemble_task = tokio::spawn(async move {
        loop {
            match seg_rx.try_recv() {
                Ok((segment_id, segment_data)) => {
                    let final_data = if encrypt {
                        if let Some(ref session) = crypto {
                            let session = session.lock().await;
                            session.decrypt(&segment_data).unwrap_or(segment_data)
                        } else {
                            segment_data
                        }
                    } else {
                        segment_data
                    };
                    decrypt_segments.lock().await.insert(segment_id, final_data);
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => break,
            }
        }
    });

    // ─────────────────────────────────────────────────────────────────
    // 모니터링 + NACK + FlowControl 루프 (메인 스레드)
    // ─────────────────────────────────────────────────────────────────
    let mut nack_count = 0u64;
    let mut last_progress_time = Instant::now();
    let mut flow_control_time = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let assembled_count = assembled_count_atomic.load(std::sync::atomic::Ordering::Relaxed) as usize;
        let last_chunk_elapsed_us = last_chunk_time.load(std::sync::atomic::Ordering::Relaxed);
        let last_chunk_age = start.elapsed().as_micros() as u64 - last_chunk_elapsed_us;
        let last_chunk_age_dur = Duration::from_micros(last_chunk_age);

        // 진행률 표시 (0.5초마다)
        if last_progress_time.elapsed() > Duration::from_millis(500) {
            let progress = (assembled_count as f64 / expected_segments as f64) * 100.0;
            let elapsed = start.elapsed().as_secs_f64();
            let total_bytes = assembled_count * segment_size;
            let speed = total_bytes as f64 / elapsed / 1024.0 / 1024.0;
            let wc = total_chunks_received.load(std::sync::atomic::Ordering::Relaxed);
            info!(
                "📊 수신: {:.1}% | seg {}/{} | {:.2} MB/s | chunks:{}",
                progress.min(100.0), assembled_count, expected_segments,
                speed, wc
            );
            last_progress_time = Instant::now();
        }

        // 흐름 제어 메시지 전송 (200ms마다) - stale 세그먼트만 손실로 카운트
        if flow_control_time.elapsed() > Duration::from_millis(200) {
            let status_map = segment_status_snapshot.read();
            let seg_times = segment_last_chunk_time.read();
            let incomplete_segments = status_map.len();
            let now_fc = Instant::now();

            let mut total_expected = 0u64;
            let mut total_missing = 0u64;
            for (seg_id, (received_bits, total)) in status_map.iter() {
                if !received_bits.iter().all(|&r| r) {
                    let is_stale = seg_times.get(seg_id)
                        .map(|t| now_fc.duration_since(*t) > Duration::from_millis(300))
                        .unwrap_or(false);
                    if is_stale {
                        let expected = *total as u64;
                        let received = received_bits.iter().filter(|&&r| r).count() as u64;
                        total_expected += expected;
                        total_missing += expected.saturating_sub(received);
                    }
                }
            }
            let loss_rate = if total_expected > 0 {
                (total_missing as f32 / total_expected as f32).min(1.0)
            } else {
                0.0
            };

            // raw 소켓 수신 속도 (bytes/sec) - 파싱과 무관한 순수 수신 대역폭
            let raw_bytes = raw_bytes_received.load(std::sync::atomic::Ordering::Relaxed);
            let elapsed_secs = start.elapsed().as_secs_f64();
            let raw_recv_rate = if elapsed_secs > 0.0 {
                raw_bytes as f32 / elapsed_secs as f32
            } else {
                0.0
            };

            // processing_rate 필드에 raw recv rate (bytes/sec)를 전달
            // 서버 BBR이 실제 수신 대역폭을 알 수 있도록
            let fc = FlowControlMessage::new(
                assembled_count as u32,
                assembled_count as u64,
                incomplete_segments as u32,
                loss_rate,
                raw_recv_rate,
            );
            let _ = send_tx.try_send(fc.to_bytes());
            flow_control_time = Instant::now();
        }

        // 완료 체크
        if assembled_count >= expected_segments {
            info!("📦 모든 세그먼트 수신 완료");
            break;
        }

        // NACK 전송 (데이터가 잠시 안오면)
        if last_chunk_age_dur > Duration::from_millis(200) {
            let status_map = segment_status_snapshot.read();

            let mut nacks_sent = 0;
            let mut total_chunks_requested = 0u64;

            for (segment_id, (received_bits, total_chunks)) in status_map.iter() {
                if !received_bits.iter().all(|&r| r) {
                    let missing: Vec<u32> = (0..*total_chunks)
                        .filter(|&i| i < received_bits.len() as u32 && !received_bits[i as usize])
                        .collect();

                    if !missing.is_empty() {
                        total_chunks_requested += missing.len() as u64;
                        let nack = NackMessage::new(*segment_id, missing, 0.0, 0);
                        let _ = send_tx.try_send(nack.to_bytes());
                        nack_count += 1;
                        nacks_sent += 1;
                        if nacks_sent >= 50 { break; }
                    }
                }
            }

            // 아직 데이터를 전혀 받지 못한 세그먼트 요청
            if nacks_sent < 50 {
                let completed_set = assembled_segments.read();
                for seg_id in 1..=expected_segments as u64 {
                    if !completed_set.contains(&seg_id) && !status_map.contains_key(&seg_id) {
                        let all_chunks: Vec<u32> = (0..chunks_per_segment as u32).collect();
                        total_chunks_requested += chunks_per_segment as u64;
                        let nack = NackMessage::new(seg_id, all_chunks, 0.0, 0);
                        let _ = send_tx.try_send(nack.to_bytes());
                        nack_count += 1;
                        nacks_sent += 1;
                        if nacks_sent >= 50 { break; }
                    }
                }
            }

            if nacks_sent > 0 {
                info!("📨 NACK: {}개 세그먼트 / {}개 청크 요청", nacks_sent, total_chunks_requested);
            }
        }

        // 10초간 새 데이터 없고 95% 이상 받았으면 종료
        if last_chunk_age_dur > Duration::from_secs(10) {
            let progress = assembled_count as f64 / expected_segments as f64;
            if progress >= 0.95 {
                info!("✅ 95% 이상 수신 완료, 종료");
                break;
            }
        }

        // 60초간 새 데이터 없으면 종료
        if last_chunk_age_dur > Duration::from_secs(60) {
            info!("⏱️  60초간 새 데이터 없음, 종료");
            break;
        }

        // 전체 타임아웃
        let total_timeout_secs = ((total_file_size as u64 / (3 * 1024 * 1024)) + 120).max(180);
        if start.elapsed() > Duration::from_secs(total_timeout_secs) {
            info!("⏱️  전체 타임아웃 ({}초)", total_timeout_secs);
            break;
        }
    }

    // 파이프라인 종료
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = recv_thread.join();
    drop(assemble_task);

    // 세그먼트 순서대로 조립
    let final_segments = decrypted_segments.lock().await;
    let mut received_data = Vec::with_capacity(total_file_size);
    let mut sorted_ids: Vec<u64> = final_segments.keys().copied().collect();
    sorted_ids.sort();
    
    for segment_id in sorted_ids {
        if let Some(data) = final_segments.get(&segment_id) {
            received_data.extend_from_slice(data);
        }
    }
    // 실제 파일 크기에 맞춰 트리밍 (사전 할당 버퍼가 약간 클 수 있음)
    received_data.truncate(total_file_size);

    let elapsed = start.elapsed();
    let throughput = received_data.len() as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    // 실제 전송 성공률 계산
    let success_rate = if total_file_size > 0 {
        (received_data.len() as f64 / total_file_size as f64 * 100.0).min(100.0)
    } else {
        0.0
    };

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("✅ 수신 완료!");
    info!("   시간: {:.2}s", elapsed.as_secs_f64());
    info!("   세그먼트: {}/{}", final_segments.len(), expected_segments);
    info!("   청크: {}", total_chunks_received.load(std::sync::atomic::Ordering::Relaxed));
    info!("   수신 크기: {:.2} MB / {:.2} MB", 
        received_data.len() as f64 / 1024.0 / 1024.0,
        total_file_size as f64 / 1024.0 / 1024.0);
    info!("   전송 성공률: {:.2}%", success_rate);
    info!("   처리량: {:.2} MB/s", throughput);
    info!("   NACK 전송 횟수: {}", nack_count);
    if encrypt {
        info!("   암호화: ChaCha20-Poly1305");
    }
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    Ok(received_data)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 로깅 설정
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args: Vec<String> = std::env::args().collect();

    let mut size_mb = 10usize;
    let mut is_server = false;
    let mut is_client = false;
    let mut addr: SocketAddr = "127.0.0.1:9000".parse()?;
    let mut encrypt = false;
    let mut num_workers = num_cpus();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--size" => {
                if i + 1 < args.len() {
                    size_mb = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--server" => is_server = true,
            "--client" => is_client = true,
            "--addr" | "--bind" | "-b" => {
                if i + 1 < args.len() {
                    addr = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--encrypt" | "-e" => encrypt = true,
            "--workers" | "-w" => {
                if i + 1 < args.len() {
                    num_workers = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!(r#"
대용량 파일 전송 테스트 (병렬 처리 + 암호화 지원)

사용법:
  cargo run --release --example large_file_test -- [OPTIONS]

옵션:
  --size <MB>       테스트 데이터 크기 (MB, 기본: 10)
  --server          서버 모드로 실행
  --client          클라이언트 모드로 실행  
  --bind, -b <ADDR> 서버: 바인드 주소 / 클라이언트: 서버 주소 (기본: 127.0.0.1:9000)
  --encrypt, -e     암호화 활성화 (X25519 + ChaCha20-Poly1305)
  --workers <N>     병렬 워커 수 (기본: CPU 코어 수)

예시:
  # 서버 (외부 접속 허용)
  cargo run --release --example large_file_test -- --server --size 100 --bind 0.0.0.0:9000

  # 클라이언트 (원격 서버 접속)
  cargo run --release --example large_file_test -- --client --size 100 --bind 192.168.1.100:9000

  # 암호화 전송
  cargo run --release --example large_file_test -- --server --size 100 --encrypt --bind 0.0.0.0:9000
  cargo run --release --example large_file_test -- --client --size 100 --encrypt --bind 192.168.1.100:9000
"#);
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    // 설정
    let mut config = Config::default();
    config.chunk_size = 1200;
    config.segment_size = 65536;  // 64KB
    config.base_redundancy_ratio = 0.20;  // 20% 중복
    config.nack_timeout_ms = 100;  // NACK 체크 주기
    config.segment_timeout_ms = 30000;  // 30초 세그먼트 타임아웃
    config.encryption_enabled = encrypt;
    config.parallel_workers = num_workers;

    let _data_size = size_mb * 1024 * 1024;

    if is_server {
        // 서버 모드
        info!("═══════════════════════════════════════════");
        info!("  SFP 대용량 전송 테스트 - 서버");
        if encrypt {
            info!("  🔐 암호화: X25519 + ChaCha20-Poly1305");
        }
        info!("═══════════════════════════════════════════");

        let data = generate_test_text(size_mb);
        run_server(addr, data, config, encrypt, num_workers).await?;

    } else if is_client {
        // 클라이언트 모드
        info!("═══════════════════════════════════════════");
        info!("  SFP 대용량 전송 테스트 - 클라이언트");
        if encrypt {
            info!("  🔐 암호화: X25519 + ChaCha20-Poly1305");
        }
        info!("═══════════════════════════════════════════");

        let received = run_client(addr, encrypt).await?;

        // 데이터 일부 출력 (확인용)
        if !received.is_empty() {
            let preview_len = received.len().min(500);
            if let Ok(preview) = std::str::from_utf8(&received[..preview_len]) {
                info!("📝 수신 데이터 미리보기 (처음 {}자):", preview_len);
                for line in preview.lines().take(5) {
                    info!("   {}", line);
                }
            }
        }

    } else {
        // 둘 다 아니면 도움말 출력
        println!("--server 또는 --client 옵션을 지정하세요. --help로 도움말 확인.");
    }

    Ok(())
}

/// CPU 코어 수 반환
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
