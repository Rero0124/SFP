//! 프로토콜 설정

use crate::{DEFAULT_CHUNK_SIZE, DEFAULT_SEGMENT_SIZE};

/// SLS 프로토콜 설정
#[derive(Debug, Clone)]
pub struct Config {
    /// 청크 크기 (바이트)
    pub chunk_size: usize,

    /// 세그먼트 크기 (바이트)
    pub segment_size: usize,

    /// 기본 중복 전송 비율 (0.0 ~ 1.0)
    /// 예: 0.2 = 20% 추가 전송
    pub base_redundancy_ratio: f64,

    /// 최대 중복 전송 비율
    pub max_redundancy_ratio: f64,

    /// 최소 중복 전송 비율
    pub min_redundancy_ratio: f64,

    /// NACK 대기 타임아웃 (밀리초)
    pub nack_timeout_ms: u64,

    /// 세그먼트 완료 대기 타임아웃 (밀리초)
    pub segment_timeout_ms: u64,

    /// 청크 전송 간격 (마이크로초)
    /// 0이면 최대 속도로 전송
    pub chunk_interval_us: u64,

    /// NIC별 속도 측정 윈도우 (청크 수)
    pub stats_window_size: usize,

    /// NIC 비율 재조정 주기 (밀리초)
    pub ratio_adjust_interval_ms: u64,

    /// 최대 동시 세그먼트 수
    pub max_concurrent_segments: usize,

    /// 수신 버퍼 크기
    pub recv_buffer_size: usize,

    /// 송신 버퍼 크기
    pub send_buffer_size: usize,

    /// 암호화 활성화 (선택)
    /// X25519 키 교환 + ChaCha20-Poly1305 사용
    pub encryption_enabled: bool,

    /// 병렬 처리 워커 수 (0이면 CPU 코어 수 사용)
    pub parallel_workers: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            segment_size: DEFAULT_SEGMENT_SIZE,
            base_redundancy_ratio: 0.15,      // 15% 기본 중복
            max_redundancy_ratio: 0.25,       // 최대 25% (NACK가 나머지 복구)
            min_redundancy_ratio: 0.05,       // 최소 5%
            nack_timeout_ms: 50,              // 50ms
            segment_timeout_ms: 5000,         // 5초
            chunk_interval_us: 0,             // 최대 속도
            stats_window_size: 100,           // 100개 청크 기준
            ratio_adjust_interval_ms: 100,    // 100ms마다 재조정
            max_concurrent_segments: 16,
            recv_buffer_size: 2 * 1024 * 1024, // 2MB
            send_buffer_size: 2 * 1024 * 1024, // 2MB
            encryption_enabled: false,        // 암호화 비활성화 (기본)
            parallel_workers: 0,              // CPU 코어 수 사용
        }
    }
}

impl Config {
    /// 새 설정 생성
    pub fn new() -> Self {
        Self::default()
    }

    /// 세그먼트당 청크 수 계산
    pub fn chunks_per_segment(&self) -> usize {
        (self.segment_size + self.chunk_size - 1) / self.chunk_size
    }

    /// 손실률 기반 중복 비율 계산
    pub fn calculate_redundancy(&self, loss_rate: f64) -> f64 {
        // 손실률이 높을수록 중복 비율 증가
        // loss_rate 0.0 -> base_redundancy
        // loss_rate 0.3 -> max_redundancy에 가까워짐
        let ratio = self.base_redundancy_ratio + (loss_rate * 2.0);
        ratio.clamp(self.min_redundancy_ratio, self.max_redundancy_ratio)
    }

    /// 저사양 기기용 설정
    pub fn low_spec() -> Self {
        Self {
            chunk_size: 1100,
            segment_size: 32768,              // 32KB
            base_redundancy_ratio: 0.20,      // 20%
            max_redundancy_ratio: 0.60,
            min_redundancy_ratio: 0.10,
            nack_timeout_ms: 100,
            segment_timeout_ms: 10000,
            chunk_interval_us: 100,           // 약간의 간격
            stats_window_size: 50,
            ratio_adjust_interval_ms: 200,
            max_concurrent_segments: 4,
            recv_buffer_size: 512 * 1024,     // 512KB
            send_buffer_size: 512 * 1024,
            encryption_enabled: false,
            parallel_workers: 2,              // 저사양은 2 워커
        }
    }

    /// 고성능 기기용 설정
    pub fn high_performance() -> Self {
        Self {
            chunk_size: 1400,
            segment_size: 131072,             // 128KB
            base_redundancy_ratio: 0.10,      // 10%
            max_redundancy_ratio: 0.50,
            min_redundancy_ratio: 0.05,
            nack_timeout_ms: 30,
            segment_timeout_ms: 3000,
            chunk_interval_us: 0,             // 최대 속도
            stats_window_size: 200,
            ratio_adjust_interval_ms: 50,
            max_concurrent_segments: 32,
            recv_buffer_size: 8 * 1024 * 1024, // 8MB
            send_buffer_size: 8 * 1024 * 1024,
            encryption_enabled: false,
            parallel_workers: 0,              // 모든 코어 사용
        }
    }

    /// 불안정한 네트워크용 설정
    pub fn unstable_network() -> Self {
        Self {
            chunk_size: 1000,                 // 작은 청크
            segment_size: 32768,              // 32KB
            base_redundancy_ratio: 0.35,      // 35%
            max_redundancy_ratio: 0.80,
            min_redundancy_ratio: 0.20,
            nack_timeout_ms: 200,
            segment_timeout_ms: 15000,
            chunk_interval_us: 50,
            stats_window_size: 30,
            ratio_adjust_interval_ms: 150,
            max_concurrent_segments: 8,
            recv_buffer_size: 1024 * 1024,
            send_buffer_size: 1024 * 1024,
            encryption_enabled: false,
            parallel_workers: 4,
        }
    }
}
