#[derive(Debug)]
pub struct BbrLite {
    pub pacing_rate: f64,   // bytes/sec (현재 추정 대역폭)
    pub min_rtt: f64,       // seconds (최소 RTT)
    pub last_rtt: f64,      // seconds (최근 RTT)
    pub delivered_bytes: u64,  // 누적 전달 바이트
    pub delivered_prev: u64,   // 이전 업데이트 시점 누적 바이트
    pub last_ts: std::time::Instant,  // 마지막 업데이트 시간

    // parameters
    pub probe_interval: f64,  // 업데이트 주기 (초)
}

impl BbrLite {
    pub fn new(initial_rtt: f64, initial_rate: f64) -> Self {
        Self {
            pacing_rate: initial_rate,
            min_rtt: initial_rtt,
            last_rtt: initial_rtt,
            delivered_bytes: 0,
            delivered_prev: 0,
            last_ts: std::time::Instant::now(),
            probe_interval: 0.05, // 50ms (더 자주 업데이트)
        }
    }

    // 호출 위치: 송신 성공 시
    pub fn on_packet_sent(&mut self, bytes: usize) {
        self.delivered_bytes += bytes as u64;
    }

    // 호출 위치: RTT 샘플 도착 시
    pub fn on_rtt_update(&mut self, rtt: f64) {
        self.last_rtt = rtt;
        if rtt < self.min_rtt {
            self.min_rtt = rtt;
        }
    }

    // 호출 위치: 주기적 (예: 50~100ms )
    pub fn update_rate(&mut self) {
        let now = std::time::Instant::now();
        let dt = now.duration_since(self.last_ts).as_secs_f64();

        if dt < self.probe_interval {
            return; // 아직 갱신할 때 아님
        }

        let delivered = self.delivered_bytes - self.delivered_prev;
        
        // 올바른 대역폭 계산: bytes / time_interval
        let delivery_rate = if dt > 0.0 {
            (delivered as f64 / dt).max(1.0)
        } else {
            1.0
        };

        self.delivered_prev = self.delivered_bytes;
        self.last_ts = now;

        // BBR의 핵심: 병목 대역폭 추정
        // btlbw = delivered / last_rtt (BBR 모델)
        let btlbw = if self.last_rtt > 0.0 && delivered > 0 {
            delivered as f64 / self.last_rtt
        } else {
            self.pacing_rate
        };

        // 큐 지표: 현재 RTT와 최소 RTT의 비율
        let queue_ratio = if self.min_rtt > 0.0 {
            self.last_rtt / self.min_rtt
        } else {
            1.0
        };

        // 이득 계산: 큐가 크면 작게, 적으면 크게
        let gain = if queue_ratio > 1.0 {
            0.9  // 큐 있음 → 송률 감소
        } else {
            1.1  // 큐 없음 → 송률 증가
        };

        // 안전한 rate 업데이트: 곱하기 대신 가중평균
        self.pacing_rate = self.pacing_rate * 0.7 + btlbw * gain * 0.3;

        // delivery_rate를 최소값으로 보장
        self.pacing_rate = self.pacing_rate.max(delivery_rate * 0.9);

        // 상한/하한 (10MB/s ~ 5GB/s)
        self.pacing_rate = self.pacing_rate.clamp(10_000_000.0, 5_000_000_000.0);
    }

    // pacing delay 계산: 다음 패킷 전송까지 대기 시간
    pub fn pacing_delay(&self, packet_size: usize) -> std::time::Duration {
        if self.pacing_rate <= 0.0 {
            return std::time::Duration::from_millis(1);
        }
        
        let delay_sec = (packet_size as f64 / self.pacing_rate).max(0.000_001);
        std::time::Duration::from_secs_f64(delay_sec)
    }
}