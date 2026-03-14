//! 실시간 스트리밍 송수신 테스트 (BBR + 동적 Redundancy)
//!
//! 사용법:
//!   cargo run --release --example realtime_test -- [OPTIONS]
//!
//! 옵션:
//!   --server          서버 모드 (데이터 생성 + 송신)
//!   --client          클라이언트 모드 (수신 + 검증)
//!   --bind, -b <ADDR> 바인드/서버 주소 (기본: 127.0.0.1:9100)
//!   --duration <SEC>  스트리밍 지속 시간 (초, 기본: 30)
//!   --rate <MB/s>     목표 전송률 (MB/s, 기본: 10)
//!
//! 예시:
//!   # 서버 (30초간 10MB/s 실시간 스트리밍)
//!   cargo run --release --example realtime_test -- --server --duration 30 --rate 10
//!
//!   # 클라이언트
//!   cargo run --release --example realtime_test -- --client

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use sfp::bbr::BbrLite;
use sfp::chunk::SegmentBuilder;
use sfp::message::{
    FlowControlMessage, InitAckMessage, InitMessage, MessageHeader, MessageType, NackMessage,
    SegmentCompleteMessage,
};
use sfp::Config;

/// 실시간 데이터 프레임 생성 (시퀀스 번호 포함)
fn generate_frame(frame_id: u64, frame_size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(frame_size);
    // 프레임 헤더: frame_id (8 bytes) + timestamp_us (8 bytes)
    data.extend_from_slice(&frame_id.to_le_bytes());
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    data.extend_from_slice(&ts.to_le_bytes());

    // 패턴 데이터 채우기
    let pattern = format!("FRAME-{:08}|", frame_id);
    let pattern_bytes = pattern.as_bytes();
    while data.len() < frame_size {
        let remaining = frame_size - data.len();
        let copy_len = remaining.min(pattern_bytes.len());
        data.extend_from_slice(&pattern_bytes[..copy_len]);
    }
    data.truncate(frame_size);
    data
}

/// 서버 (실시간 송신자) 실행
async fn run_server(
    addr: SocketAddr,
    config: Config,
    duration_secs: u64,
    target_rate_mbps: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let socket = Arc::new(UdpSocket::bind(addr).await?);
    info!("📡 실시간 서버 시작: {}", addr);
    info!("⏱️  스트리밍 시간: {}초", duration_secs);
    info!("🎯 목표 전송률: {:.1} MB/s", target_rate_mbps);
    info!("⚙️  세그먼트 크기: {} bytes", config.segment_size);

    // 송신 큐
    let (priority_tx, mut priority_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(1000);
    let (data_tx, mut data_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(100_000);

    // 송신 태스크
    let send_socket = socket.clone();
    let _send_task = tokio::spawn(async move {
        loop {
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

    // 수신 큐
    let (recv_tx, recv_rx) = mpsc::channel::<(Vec<u8>, SocketAddr)>(100_000);
    let recv_rx = Arc::new(Mutex::new(recv_rx));
    let recv_socket = socket.clone();
    let _recv_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        loop {
            match recv_socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    let _ = recv_tx.try_send((buf[..len].to_vec(), addr));
                }
                Err(_) => break,
            }
        }
    });

    info!("⏳ 클라이언트 연결 대기 중...");

    // Init 대기
    let client_addr = loop {
        let mut rx = recv_rx.lock().await;
        if let Some((data, addr)) = rx.recv().await {
            drop(rx);
            if let Ok(header) =
                bincode::deserialize::<MessageHeader>(&data[..data.len().min(32)])
            {
                if header.msg_type == MessageType::Init {
                    info!("✅ 클라이언트 연결: {}", addr);
                    break addr;
                }
            }
        }
    };

    // InitAck 전송 (total_file_size=0 → 실시간 스트리밍 모드)
    let ack = InitAckMessage::new(
        0, // 실시간 모드: 파일 크기 미지정
        config.chunk_size as u16,
        config.segment_size as u32,
        config.base_redundancy_ratio as f32,
    );
    let _ = priority_tx.send((ack.to_bytes(), client_addr)).await;

    // BBR 초기화 (보수적 시작 → Startup에서 점진 증가)
    let segment_builder = Arc::new(SegmentBuilder::new(config.chunk_size));
    let _packet_size = config.chunk_size + 100;
    let initial_rate = (target_rate_mbps * 1024.0 * 1024.0).min(5_000_000.0); // 최대 5MB/s로 시작
    let bbr = Arc::new(Mutex::new(BbrLite::new(0.005, initial_rate)));

    // 세그먼트 청크 캐시 (재전송용)
    let segment_chunks: Arc<RwLock<HashMap<u64, Vec<sfp::chunk::Chunk>>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // Redundancy 설정
    let base_redundancy = config.base_redundancy_ratio;
    let min_redundancy = config.min_redundancy_ratio;
    let max_redundancy = config.max_redundancy_ratio;

    // 완료 세그먼트 추적
    let completed_segments: Arc<RwLock<std::collections::HashSet<u64>>> =
        Arc::new(RwLock::new(std::collections::HashSet::new()));

    // FlowControl + NACK 수신 태스크
    let fc_bbr = bbr.clone();
    let fc_recv_rx = recv_rx.clone();
    let fc_completed = completed_segments.clone();
    let nack_chunks_cache = segment_chunks.clone();
    let nack_data_tx = data_tx.clone();
    let nack_client_addr = client_addr;
    let fc_base = base_redundancy;
    let fc_min = min_redundancy;
    let fc_max = max_redundancy;

    let streaming_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fc_done = streaming_done.clone();

    let retransmit_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let fc_retransmit = retransmit_count.clone();

    let _fc_task = tokio::spawn(async move {
        let mut last_log = Instant::now();

        while !fc_done.load(std::sync::atomic::Ordering::Relaxed) {
            let mut rx = fc_recv_rx.lock().await;
            match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
                Ok(Some((data, _addr))) => {
                    drop(rx);

                    // FlowControl 메시지 처리
                    if let Some(fc) = FlowControlMessage::from_bytes(&data) {
                        let mut b = fc_bbr.lock().await;
                        b.on_loss_update(fc.loss_rate as f64);
                        if fc.processing_rate > 0.0 {
                            let receiver_rate = fc.processing_rate as f64 * 65536.0;
                            b.on_receiver_rate_update(receiver_rate);
                        }
                        if fc.segments_in_progress > 0 {
                            let queue_delay = fc.segments_in_progress as f64 * 0.0005;
                            b.on_rtt_update(0.005 + queue_delay);
                        } else {
                            b.on_rtt_update(0.005);
                        }
                        b.update_rate();

                        if last_log.elapsed() > Duration::from_millis(1000) {
                            let dyn_red =
                                b.recommended_redundancy(fc_base, fc_min, fc_max);
                            info!(
                                "📶 BBR {:?} rate:{:.0}MB/s loss:{:.1}% redundancy:{:.0}%",
                                b.state,
                                b.pacing_rate / 1024.0 / 1024.0,
                                b.loss_rate * 100.0,
                                dyn_red * 100.0
                            );
                            last_log = Instant::now();
                        }
                        continue;
                    }

                    // SegmentComplete 메시지
                    if let Some(msg) = SegmentCompleteMessage::from_bytes(&data) {
                        fc_completed.write().await.insert(msg.segment_id);
                        continue;
                    }

                    // NACK 처리 + 재전송
                    if let Some(nack) = NackMessage::from_bytes(&data) {
                        {
                            let mut b = fc_bbr.lock().await;
                            let loss_ratio =
                                nack.missing_chunk_ids.len() as f64 / 55.0;
                            b.on_loss_update(loss_ratio);
                            b.update_rate();
                        }

                        let cache = nack_chunks_cache.read().await;
                        if let Some(chunks) = cache.get(&nack.segment_id) {
                            for &chunk_id in &nack.missing_chunk_ids {
                                if let Some(chunk) = chunks.get(chunk_id as usize) {
                                    let bytes = chunk.to_bytes();
                                    let _ = nack_data_tx
                                        .send((bytes, nack_client_addr))
                                        .await;
                                    fc_retransmit.fetch_add(
                                        1,
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    drop(rx);
                }
            }
        }
    });

    // 실시간 스트리밍 시작
    info!("🚀 실시간 스트리밍 시작 ({}초)...", duration_secs);
    let stream_start = Instant::now();
    let frame_size = config.segment_size; // 1 frame = 1 segment
    let target_bytes_per_sec = target_rate_mbps * 1024.0 * 1024.0;
    let target_interval = Duration::from_secs_f64(frame_size as f64 / target_bytes_per_sec);

    let mut segment_id = 1u64;
    let mut total_bytes_sent = 0u64;
    let mut total_chunks = 0u64;
    let mut total_redundant = 0u64;
    let mut last_stats_time = Instant::now();

    let max_cache_segments = 500u64; // 최대 캐시 크기

    while stream_start.elapsed() < Duration::from_secs(duration_secs) {
        let frame_start = Instant::now();

        // 백프레셔: 미완료 프레임이 너무 많으면 대기
        let max_inflight = 50u64;
        loop {
            let completed = completed_segments.read().await.len() as u64;
            let inflight = segment_id.saturating_sub(completed + 1);
            if inflight < max_inflight {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // 프레임 데이터 생성
        let frame_data = generate_frame(segment_id, frame_size);

        // BBR 기반 동적 redundancy 계산
        let redundancy_ratio = {
            let b = bbr.lock().await;
            b.recommended_redundancy(base_redundancy, min_redundancy, max_redundancy)
        };

        // 청크 분할 + redundancy 생성
        let chunks = segment_builder.split_into_chunks(segment_id, &frame_data, 0);
        let redundant_chunks =
            segment_builder.create_redundant_chunks(&chunks, redundancy_ratio);

        // 캐시 저장 (재전송용)
        {
            let mut cache = segment_chunks.write().await;
            cache.insert(segment_id, chunks.clone());
            // 오래된 캐시 정리
            if segment_id > max_cache_segments {
                cache.remove(&(segment_id - max_cache_segments));
            }
        }

        // 청크 전송 (interval 기반 BBR pacing)
        const PACING_INTERVAL: Duration = Duration::from_millis(50);

        let all_chunks: Vec<_> = chunks.iter().chain(redundant_chunks.iter()).collect();
        let mut chunk_idx = 0;
        let mut interval_start = Instant::now();
        let mut interval_bytes_sent = 0usize;

        let mut bytes_budget = {
            let b = bbr.lock().await;
            b.bytes_per_interval(PACING_INTERVAL)
        };

        while chunk_idx < all_chunks.len() {
            let chunk = all_chunks[chunk_idx];
            let bytes = chunk.to_bytes();
            let byte_len = bytes.len();
            let _ = data_tx.send((bytes, client_addr)).await;

            if chunk_idx < chunks.len() {
                total_chunks += 1;
            } else {
                total_redundant += 1;
            }
            interval_bytes_sent += byte_len;
            chunk_idx += 1;

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
        if interval_bytes_sent > 0 {
            let mut b = bbr.lock().await;
            b.on_packet_sent(interval_bytes_sent);
        }

        total_bytes_sent += frame_size as u64;

        // 주기적 통계
        if last_stats_time.elapsed() > Duration::from_secs(3) {
            let elapsed = stream_start.elapsed().as_secs_f64();
            let speed = total_bytes_sent as f64 / elapsed / 1024.0 / 1024.0;
            let completed = completed_segments.read().await.len();
            let retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);
            let b = bbr.lock().await;
            info!(
                "📊 [{:.0}s] frames:{} | {:.1}MB/s | completed:{} | retrans:{} | {:?} red:{:.0}%",
                elapsed,
                segment_id,
                speed,
                completed,
                retrans,
                b.state,
                redundancy_ratio * 100.0
            );
            last_stats_time = Instant::now();
        }

        segment_id += 1;

        // 목표 전송률에 맞춰 대기
        let frame_elapsed = frame_start.elapsed();
        if frame_elapsed < target_interval {
            tokio::time::sleep(target_interval - frame_elapsed).await;
        }
    }

    // 스트리밍 종료 → NACK 후처리 대기
    let total_frames = segment_id - 1;
    info!("⏳ NACK 후처리 대기 중 (최대 30초)...");

    let nack_wait_start = Instant::now();
    let mut last_retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);
    let mut idle_time = Instant::now();

    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let completed = completed_segments.read().await.len() as u64;
        let retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);

        // 모두 완료
        if completed >= total_frames {
            info!("✅ 모든 프레임 완료 확인");
            break;
        }
        // 재전송 활동이 있으면 타이머 리셋
        if retrans != last_retrans {
            idle_time = Instant::now();
            last_retrans = retrans;
        }
        // 10초간 재전송 없으면 종료
        if idle_time.elapsed() > Duration::from_secs(10) {
            info!("⏱️  10초간 재전송 없음, 종료");
            break;
        }
        // 전체 30초 초과
        if nack_wait_start.elapsed() > Duration::from_secs(30) {
            info!("⏱️  NACK 후처리 타임아웃 (30초)");
            break;
        }
    }

    streaming_done.store(true, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let elapsed = stream_start.elapsed();
    let final_speed = total_bytes_sent as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;
    let completed = completed_segments.read().await.len();
    let retrans = retransmit_count.load(std::sync::atomic::Ordering::Relaxed);

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("🏁 실시간 스트리밍 완료");
    info!("   시간: {:.2}s", elapsed.as_secs_f64());
    info!("   프레임: {} 전송", total_frames);
    info!(
        "   총 데이터: {:.2} MB",
        total_bytes_sent as f64 / 1024.0 / 1024.0
    );
    info!("   평균 속도: {:.2} MB/s", final_speed);
    info!(
        "   청크: {} (원본) + {} (중복)",
        total_chunks, total_redundant
    );
    info!("   재전송: {} 청크", retrans);
    info!(
        "   완료 확인: {}/{} ({:.1}%)",
        completed,
        total_frames,
        completed as f64 / total_frames as f64 * 100.0
    );
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    Ok(())
}

/// 클라이언트 (실시간 수신자) 실행
async fn run_client(
    server_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("📡 실시간 클라이언트 시작");
    info!("🎯 서버: {}", server_addr);

    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);

    // 송신 큐
    let (send_tx, mut send_rx) = mpsc::channel::<Vec<u8>>(1000);
    let send_socket = socket.clone();
    let _send_task = tokio::spawn(async move {
        while let Some(bytes) = send_rx.recv().await {
            let _ = send_socket.send_to(&bytes, server_addr).await;
        }
    });

    // 수신 큐
    let (recv_tx, recv_rx) = mpsc::channel::<Vec<u8>>(100_000);
    let recv_rx = Arc::new(Mutex::new(recv_rx));
    let recv_socket = socket.clone();
    let _recv_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        loop {
            match recv_socket.recv_from(&mut buf).await {
                Ok((len, _)) => {
                    let _ = recv_tx.try_send(buf[..len].to_vec());
                }
                Err(_) => break,
            }
        }
    });

    // Init 핸드쉐이크
    let init_msg = InitMessage::new(false, [0u8; 32]);
    info!("📤 Init 전송...");

    let init_ack: InitAckMessage = loop {
        let _ = send_tx.send(init_msg.to_bytes()).await;
        let mut rx = recv_rx.lock().await;
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(data)) => {
                drop(rx);
                if let Some(ack) = InitAckMessage::from_bytes(&data) {
                    break ack;
                }
            }
            Ok(None) => return Err("수신 채널 종료".into()),
            Err(_) => {
                drop(rx);
                info!("📤 Init 재전송...");
            }
        }
    };

    let segment_size = init_ack.segment_size as usize;
    let chunks_per_segment = init_ack.chunks_per_segment as usize;
    info!("✅ 연결 완료 (실시간 스트리밍 모드)");
    info!("   세그먼트 크기: {} bytes", segment_size);
    info!("   세그먼트당 청크: {}", chunks_per_segment);

    // 초기 FlowControl
    let initial_fc = FlowControlMessage::new(1000, 0, 0, 0.0, 100.0);
    let _ = send_tx.send(initial_fc.to_bytes()).await;

    let start = Instant::now();

    // 공유 상태
    let segment_chunks: Arc<RwLock<HashMap<u64, HashMap<u32, Vec<u8>>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let segment_total_chunks: Arc<RwLock<HashMap<u64, u32>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let assembled_segments: Arc<RwLock<std::collections::HashSet<u64>>> =
        Arc::new(RwLock::new(std::collections::HashSet::new()));
    let segment_last_chunk_time: Arc<RwLock<HashMap<u64, Instant>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let total_chunks_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let last_chunk_time = Arc::new(RwLock::new(Instant::now()));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

    // 프레임 수신 통계
    let frame_latencies: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));

    // 처리 워커 풀
    let num_workers = 4;
    let mut worker_handles = Vec::new();

    for _worker_id in 0..num_workers {
        let rx = recv_rx.clone();
        let last_chunk = last_chunk_time.clone();
        let chunks = segment_chunks.clone();
        let totals = segment_total_chunks.clone();
        let assembled = assembled_segments.clone();
        let chunks_count = total_chunks_received.clone();
        let worker_running = running.clone();
        let worker_send_tx = send_tx.clone();
        let seg_last_time = segment_last_chunk_time.clone();
        let latencies = frame_latencies.clone();

        let handle = tokio::spawn(async move {
            loop {
                let data = {
                    let mut rx_guard = rx.lock().await;
                    match tokio::time::timeout(Duration::from_millis(50), rx_guard.recv()).await {
                        Ok(Some(data)) => data,
                        Ok(None) => break,
                        Err(_) => {
                            if !worker_running.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            }
                            continue;
                        }
                    }
                };

                *last_chunk.write().await = Instant::now();

                if let Some(chunk) = sfp::chunk::Chunk::from_bytes(&data) {
                    let segment_id = chunk.header.segment_id;
                    let chunk_id = chunk.header.chunk_id;
                    let total_chunks = chunk.header.total_chunks;

                    totals.write().await.insert(segment_id, total_chunks);

                    let mut chunks_guard = chunks.write().await;
                    let segment = chunks_guard.entry(segment_id).or_insert_with(HashMap::new);
                    if !segment.contains_key(&chunk_id) {
                        segment.insert(chunk_id, chunk.data.to_vec());
                        chunks_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        seg_last_time.write().await.insert(segment_id, Instant::now());
                    }

                    let is_complete = segment.len() >= total_chunks as usize;
                    let already_assembled = assembled.read().await.contains(&segment_id);

                    if is_complete && !already_assembled {
                        // 프레임 조립
                        let mut frame_data =
                            Vec::with_capacity(total_chunks as usize * 1200);
                        for i in 0..total_chunks {
                            if let Some(chunk_data) = segment.get(&i) {
                                frame_data.extend_from_slice(chunk_data);
                            }
                        }
                        drop(chunks_guard);

                        assembled.write().await.insert(segment_id);

                        // 프레임 지연 시간 계산 (프레임 헤더에서 타임스탬프 추출)
                        if frame_data.len() >= 16 {
                            let frame_ts = u64::from_le_bytes(
                                frame_data[8..16].try_into().unwrap_or([0; 8]),
                            );
                            if frame_ts > 0 {
                                let now_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_micros() as u64;
                                let latency_ms =
                                    (now_ts.saturating_sub(frame_ts)) as f64 / 1000.0;
                                latencies.lock().await.push(latency_ms);
                            }
                        }

                        // 서버에 완료 알림
                        let complete_msg = SegmentCompleteMessage::new(
                            segment_id,
                            total_chunks as u32,
                            0,
                        );
                        let _ = worker_send_tx.try_send(complete_msg.to_bytes());

                        // 오래된 청크 데이터 정리
                        if segment_id > 200 {
                            let mut cg = chunks.write().await;
                            cg.remove(&(segment_id - 200));
                        }
                    }
                }
            }
        });
        worker_handles.push(handle);
    }

    // 모니터링 + NACK + FlowControl 루프
    let mut nack_count = 0u64;
    let mut last_progress_time = Instant::now();
    let mut flow_control_time = Instant::now();
    let mut last_assembled_count = 0usize;
    let mut prev_assembled_fc = 0usize;
    let mut prev_fc_elapsed = 0.0f64;

    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let assembled_count = assembled_segments.read().await.len();
        let last_chunk = *last_chunk_time.read().await;

        // 진행률 표시 (2초마다)
        if last_progress_time.elapsed() > Duration::from_secs(2) {
            let elapsed = start.elapsed().as_secs_f64();
            let frames_per_sec =
                (assembled_count - last_assembled_count) as f64 / last_progress_time.elapsed().as_secs_f64();
            let speed = assembled_count as f64 * segment_size as f64 / elapsed / 1024.0 / 1024.0;

            // 평균 지연 시간
            let avg_latency = {
                let lats = frame_latencies.lock().await;
                if lats.is_empty() {
                    0.0
                } else {
                    let recent: Vec<f64> = lats.iter().rev().take(100).copied().collect();
                    recent.iter().sum::<f64>() / recent.len() as f64
                }
            };

            info!(
                "📊 프레임:{} | {:.1}fps | {:.1}MB/s | latency:{:.1}ms | NACKs:{}",
                assembled_count, frames_per_sec, speed, avg_latency, nack_count
            );
            last_assembled_count = assembled_count;
            last_progress_time = Instant::now();
        }

        // FlowControl (200ms마다)
        if flow_control_time.elapsed() > Duration::from_millis(200) {
            let chunks_map = segment_chunks.read().await;
            let assembled_set = assembled_segments.read().await;
            let totals_map = segment_total_chunks.read().await;
            let seg_times = segment_last_chunk_time.read().await;
            let incomplete_segments = chunks_map.len() - assembled_set.len();
            let now_fc = Instant::now();

            let mut total_expected = 0u64;
            let mut total_missing = 0u64;
            for (seg_id, received_chunks) in chunks_map.iter() {
                if !assembled_set.contains(seg_id) {
                    let stale_dur = seg_times
                        .get(seg_id)
                        .map(|t| now_fc.duration_since(*t));
                    // 300ms~5s 사이만 loss로 카운트 (너무 오래된 것은 제외)
                    let is_recent_stale = stale_dur
                        .map(|d| d > Duration::from_millis(300) && d < Duration::from_secs(5))
                        .unwrap_or(false);
                    if is_recent_stale {
                        let expected = totals_map.get(seg_id).copied().unwrap_or(55) as u64;
                        let received = received_chunks.len() as u64;
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

            // Rolling window: 이전 FC 이후 처리량 기반
            let current_elapsed = start.elapsed().as_secs_f64();
            let current_assembled = assembled_set.len();
            let delta_assembled = current_assembled - prev_assembled_fc;
            let delta_time = current_elapsed - prev_fc_elapsed;
            let processing_rate = if delta_time > 0.05 {
                delta_assembled as f32 / delta_time as f32
            } else {
                0.0
            };
            prev_assembled_fc = current_assembled;
            prev_fc_elapsed = current_elapsed;

            let fc = FlowControlMessage::new(
                assembled_set.len() as u32,
                assembled_set.iter().max().copied().unwrap_or(0),
                incomplete_segments as u32,
                loss_rate,
                processing_rate,
            );
            let _ = send_tx.try_send(fc.to_bytes());
            flow_control_time = Instant::now();
        }

        // NACK 전송
        if last_chunk.elapsed() > Duration::from_millis(200) {
            let chunks_map = segment_chunks.read().await;
            let totals_map = segment_total_chunks.read().await;
            let assembled_set = assembled_segments.read().await;

            let mut nacks_sent = 0;
            for (segment_id, chunks) in chunks_map.iter() {
                if !assembled_set.contains(segment_id) {
                    let total = totals_map.get(segment_id).copied().unwrap_or(55);
                    let received: std::collections::HashSet<u32> =
                        chunks.keys().copied().collect();
                    let missing: Vec<u32> =
                        (0..total).filter(|i| !received.contains(i)).collect();
                    if !missing.is_empty() {
                        let nack = NackMessage::new(*segment_id, missing, 0.0, 0);
                        let _ = send_tx.try_send(nack.to_bytes());
                        nack_count += 1;
                        nacks_sent += 1;
                        if nacks_sent >= 30 {
                            break;
                        }
                    }
                }
            }
        }

        // 15초간 새 데이터 없으면 종료 (서버 스트리밍 끝)
        if last_chunk.elapsed() > Duration::from_secs(15) {
            info!("⏱️  15초간 새 데이터 없음, 스트리밍 종료");
            break;
        }
    }

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    for handle in worker_handles {
        let _ = handle.await;
    }

    // 최종 통계
    let elapsed = start.elapsed();
    let assembled_count = assembled_segments.read().await.len();
    let total_received = total_chunks_received.load(std::sync::atomic::Ordering::Relaxed);
    let speed = assembled_count as f64 * segment_size as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    // 지연 시간 통계
    let (avg_lat, p50_lat, p99_lat) = {
        let mut lats = frame_latencies.lock().await;
        if lats.is_empty() {
            (0.0, 0.0, 0.0)
        } else {
            lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let avg = lats.iter().sum::<f64>() / lats.len() as f64;
            let p50 = lats[lats.len() / 2];
            let p99 = lats[(lats.len() as f64 * 0.99) as usize];
            (avg, p50, p99)
        }
    };

    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    info!("🏁 실시간 수신 완료");
    info!("   시간: {:.2}s", elapsed.as_secs_f64());
    info!("   프레임 수신: {}", assembled_count);
    info!(
        "   수신 데이터: {:.2} MB",
        assembled_count as f64 * segment_size as f64 / 1024.0 / 1024.0
    );
    info!("   평균 속도: {:.2} MB/s", speed);
    info!("   총 청크: {}", total_received);
    info!("   NACK 전송: {}", nack_count);
    info!("   지연 시간:");
    info!("     평균: {:.1}ms", avg_lat);
    info!("     P50:  {:.1}ms", p50_lat);
    info!("     P99:  {:.1}ms", p99_lat);
    info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args: Vec<String> = std::env::args().collect();

    let mut is_server = false;
    let mut is_client = false;
    let mut addr: SocketAddr = "127.0.0.1:9100".parse()?;
    let mut duration_secs = 30u64;
    let mut target_rate = 10.0f64;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--server" => is_server = true,
            "--client" => is_client = true,
            "--addr" | "--bind" | "-b" => {
                if i + 1 < args.len() {
                    addr = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--duration" => {
                if i + 1 < args.len() {
                    duration_secs = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--rate" => {
                if i + 1 < args.len() {
                    target_rate = args[i + 1].parse()?;
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!(
                    r#"
실시간 스트리밍 송수신 테스트 (BBR + 동적 Redundancy)

사용법:
  cargo run --release --example realtime_test -- [OPTIONS]

옵션:
  --server          서버 모드 (데이터 생성 + 송신)
  --client          클라이언트 모드 (수신 + 검증)
  --bind, -b <ADDR> 바인드/서버 주소 (기본: 127.0.0.1:9100)
  --duration <SEC>  스트리밍 지속 시간 (초, 기본: 30)
  --rate <MB/s>     목표 전송률 (MB/s, 기본: 10)

예시:
  # 터미널 1: 서버
  cargo run --release --example realtime_test -- --server --duration 30 --rate 10

  # 터미널 2: 클라이언트
  cargo run --release --example realtime_test -- --client --bind 127.0.0.1:9100
"#
                );
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    let mut config = Config::default();
    config.chunk_size = 1200;
    config.segment_size = 65536; // 64KB per frame
    config.base_redundancy_ratio = 0.15;
    config.min_redundancy_ratio = 0.05;
    config.max_redundancy_ratio = 0.70;
    config.nack_timeout_ms = 100;
    config.segment_timeout_ms = 10000;

    if is_server {
        info!("═══════════════════════════════════════════");
        info!("  SFP 실시간 스트리밍 테스트 - 서버");
        info!("═══════════════════════════════════════════");
        run_server(addr, config, duration_secs, target_rate).await?;
    } else if is_client {
        info!("═══════════════════════════════════════════");
        info!("  SFP 실시간 스트리밍 테스트 - 클라이언트");
        info!("═══════════════════════════════════════════");
        run_client(addr).await?;
    } else {
        println!("--server 또는 --client 옵션을 지정하세요. --help로 도움말 확인.");
    }

    Ok(())
}
