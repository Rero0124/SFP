use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// BBR 상태 (Gain Cycling)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BbrState {
    /// 시작 단계: 대역폭을 빠르게 탐색 (Slow Start 대체)
    Startup,
    /// 대역폭 탐색: gain > 1.0으로 전송량 증가
    ProbeUp,
    /// 큐 배출: gain < 1.0으로 큐 비우기
    ProbeDrain,
    /// 안정 상태: gain = 1.0으로 유지
    Cruise,
}

/// 대역폭 샘플 (windowed max 추적용)
#[derive(Debug, Clone)]
struct BwSample {
    bw: f64,
    time: Instant,
}

#[derive(Debug)]
pub struct BbrLite {
    pub pacing_rate: f64,       // bytes/sec (현재 pacing rate)
    pub min_rtt: f64,           // seconds (최소 RTT - BBR의 RTprop)
    pub last_rtt: f64,          // seconds (최근 RTT)
    pub max_bw: f64,            // bytes/sec (windowed max 대역폭 - BBR의 BtlBw)
    pub delivered_bytes: u64,   // 누적 전달 바이트
    pub delivered_prev: u64,    // 이전 업데이트 시점 누적 바이트
    pub last_ts: Instant,       // 마지막 업데이트 시간

    // BBR 상태
    pub state: BbrState,
    state_start: Instant,       // 현재 상태 진입 시간

    // Windowed max bandwidth (최근 N초간 최대값 유지)
    bw_samples: VecDeque<BwSample>,
    bw_window_secs: f64,        // 윈도우 크기 (초)

    // RTT 추적
    min_rtt_timestamp: Instant, // min_rtt 측정 시점 (주기적 리셋용)
    pub rtt_sample_count: u64,  // RTT 샘플 수

    // 손실 추적
    pub loss_rate: f64,         // 현재 손실률 (0.0 ~ 1.0)

    // 수신자 피드백 기반 delivery rate
    receiver_delivery_rate: f64, // 수신자 측정 delivery rate (bytes/sec)

    // parameters
    pub probe_interval: f64,    // 업데이트 주기 (초)
    min_rate: f64,              // 최소 pacing rate (bytes/sec)
    max_rate: f64,              // 최대 pacing rate (bytes/sec)
}

impl BbrLite {
    pub fn new(initial_rtt: f64, initial_rate: f64) -> Self {
        let now = Instant::now();
        Self {
            pacing_rate: initial_rate,
            min_rtt: initial_rtt,
            last_rtt: initial_rtt,
            max_bw: 1_000_000.0, // min_rate - 수신자 피드백으로 업데이트
            delivered_bytes: 0,
            delivered_prev: 0,
            last_ts: now,
            state: BbrState::Startup,
            state_start: now,
            bw_samples: VecDeque::new(),
            bw_window_secs: 2.0, // 2초 윈도우 (빠른 대역폭 변화 추적)
            min_rtt_timestamp: now,
            rtt_sample_count: 0,
            loss_rate: 0.0,
            receiver_delivery_rate: 0.0,
            probe_interval: 0.05, // 50ms
            min_rate: 1_000_000.0,     // 1 MB/s
            max_rate: 5_000_000_000.0, // 5 GB/s
        }
    }

    /// 패킷 전송 성공 시 호출
    pub fn on_packet_sent(&mut self, bytes: usize) {
        self.delivered_bytes += bytes as u64;
    }

    /// RTT 샘플 도착 시 호출
    pub fn on_rtt_update(&mut self, rtt: f64) {
        if rtt <= 0.0 {
            return;
        }
        self.last_rtt = rtt;
        self.rtt_sample_count += 1;

        // min_rtt 업데이트 (10초마다 리셋하여 경로 변화 추적)
        if rtt < self.min_rtt || self.min_rtt_timestamp.elapsed().as_secs_f64() > 10.0 {
            self.min_rtt = rtt;
            self.min_rtt_timestamp = Instant::now();
        }
    }

    /// 손실률 업데이트
    pub fn on_loss_update(&mut self, loss_rate: f64) {
        // EWMA로 손실률 평활화
        self.loss_rate = self.loss_rate * 0.5 + loss_rate * 0.5;
    }

    /// 수신자 측 delivery rate 업데이트 (FlowControl 기반)
    pub fn on_receiver_rate_update(&mut self, rate_bytes_per_sec: f64) {
        if rate_bytes_per_sec > 0.0 {
            self.receiver_delivery_rate = rate_bytes_per_sec;
        }
    }

    /// Windowed max bandwidth 업데이트
    fn update_max_bw(&mut self, delivery_rate: f64) {
        let now = Instant::now();

        // 새 샘플 추가
        if delivery_rate > 0.0 {
            self.bw_samples.push_back(BwSample {
                bw: delivery_rate,
                time: now,
            });
        }

        // 오래된 샘플 제거 (윈도우 밖)
        while let Some(front) = self.bw_samples.front() {
            if now.duration_since(front.time).as_secs_f64() > self.bw_window_secs {
                self.bw_samples.pop_front();
            } else {
                break;
            }
        }

        // Windowed maximum 계산
        self.max_bw = self.bw_samples
            .iter()
            .map(|s| s.bw)
            .fold(self.min_rate, f64::max);
    }

    /// 주기적 rate 업데이트 (BBR 핵심)
    pub fn update_rate(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_ts).as_secs_f64();

        if dt < self.probe_interval {
            return;
        }

        // 수신자 피드백이 아직 없으면 initial_rate 유지 (rate 변경하지 않음)
        // 피드백 없이 송신자 측 delivery_rate로 계산하면 자기참조 순환으로 폭주함
        if self.receiver_delivery_rate <= 0.0 {
            self.delivered_prev = self.delivered_bytes;
            self.last_ts = now;
            return;
        }

        // 1. 송신자 측 delivery rate 계산 (bytes/sec)
        let delivered = self.delivered_bytes.saturating_sub(self.delivered_prev);
        let _sender_delivery_rate = if dt > 0.0 && delivered > 0 {
            delivered as f64 / dt
        } else {
            0.0
        };

        self.delivered_prev = self.delivered_bytes;
        self.last_ts = now;

        // 2. delivery rate 결정: 수신자 raw recv rate가 실제 대역폭
        let delivery_rate = self.receiver_delivery_rate;

        // 3. Windowed max bandwidth 업데이트
        self.update_max_bw(delivery_rate);

        // 4. 상태 전이
        let state_duration = now.duration_since(self.state_start).as_secs_f64();
        self.transition_state(state_duration);

        // 5. 상태별 gain 결정 (공격적: 정확한 손실률 기반)
        let gain = match self.state {
            BbrState::Startup => 2.0,     // 빠른 대역폭 탐색
            BbrState::ProbeUp => 1.5,     // 적극적 대역폭 재탐색
            BbrState::ProbeDrain => 0.5,  // 빠른 큐 배출
            BbrState::Cruise => 1.0,      // 안정 유지
        };

        // 6. pacing_rate 계산: max_bw * gain
        let target_rate = self.max_bw * gain;

        // 7. rate는 max_bw * gain으로만 결정 (loss는 redundancy로 대응)
        let adjusted_rate = target_rate;

        // 8. pacing_rate 업데이트
        //    손실 없고 수신자 피드백이 있으면 빠르게 수렴 (자기참조 순환 아님)
        let alpha = match self.state {
            BbrState::Startup => 0.95,   // 즉시 수렴
            BbrState::ProbeUp => 0.85,   // 빠른 수렴
            BbrState::ProbeDrain => 0.8,  // 빠른 감속
            BbrState::Cruise => 0.7,      // 빠른 안정화
        };
        self.pacing_rate = self.pacing_rate * (1.0 - alpha) + adjusted_rate * alpha;

        // 9. 상한/하한 적용
        self.pacing_rate = self.pacing_rate.clamp(self.min_rate, self.max_rate);
    }

    /// 상태 전이 로직
    fn transition_state(&mut self, state_duration: f64) {
        let queue_ratio = if self.min_rtt > 0.0 {
            self.last_rtt / self.min_rtt
        } else {
            1.0
        };

        match self.state {
            BbrState::Startup => {
                // Startup → ProbeDrain: 충분히 탐색했거나 손실/큐잉 감지
                if state_duration > 2.0 || self.loss_rate > 0.05 || queue_ratio > 2.0 {
                    self.state = BbrState::ProbeDrain;
                    self.state_start = Instant::now();
                    // max_bw를 수신자 피드백으로 보정 (샘플은 유지하되 과도한 값 억제)
                    if self.receiver_delivery_rate > 0.0 && self.max_bw > self.receiver_delivery_rate * 2.0 {
                        self.max_bw = self.receiver_delivery_rate * 1.5;
                    }
                }
            }
            BbrState::ProbeUp => {
                // ProbeUp → ProbeDrain: 큐가 쌓이면 또는 일정 시간 경과
                if queue_ratio > 1.25 || state_duration > 0.5 || self.loss_rate > 0.05 {
                    self.state = BbrState::ProbeDrain;
                    self.state_start = Instant::now();
                }
            }
            BbrState::ProbeDrain => {
                // ProbeDrain → Cruise: 큐가 비면 또는 일정 시간 경과
                if queue_ratio < 1.1 || state_duration > 0.5 {
                    self.state = BbrState::Cruise;
                    self.state_start = Instant::now();
                }
            }
            BbrState::Cruise => {
                // Cruise → ProbeUp: 주기적 대역폭 재탐색
                // 손실이 낮을수록 더 빈번하게 탐색 (무손실: 2초, 낮은 손실: 5초)
                let probe_interval = if self.loss_rate < 0.001 {
                    1.0  // 무손실: 1초마다 재탐색
                } else if self.loss_rate < 0.01 {
                    2.0
                } else {
                    3.0
                };
                if state_duration > probe_interval && self.loss_rate < 0.1 {
                    self.state = BbrState::ProbeUp;
                    self.state_start = Instant::now();
                }
            }
        }
    }

    /// pacing delay 계산: 다음 패킷 전송까지 대기 시간
    /// 100μs 미만의 delay는 OS 타이머 해상도(~1ms)보다 작아 실용적이지 않으므로
    /// Duration::ZERO를 반환하여 불필요한 sleep을 방지
    pub fn pacing_delay(&self, packet_size: usize) -> Duration {
        if self.pacing_rate <= 0.0 {
            return Duration::from_millis(1);
        }

        let delay_sec = packet_size as f64 / self.pacing_rate;
        if delay_sec < 0.0001 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(delay_sec)
        }
    }

    /// 고정 간격(interval) 동안 전송 가능한 바이트 수 계산
    /// interval-based pacing: 일정 주기마다 전송량을 제어
    pub fn bytes_per_interval(&self, interval: Duration) -> usize {
        if self.pacing_rate <= 0.0 {
            return 0;
        }
        (self.pacing_rate * interval.as_secs_f64()) as usize
    }

    /// BDP (Bandwidth-Delay Product) 계산
    pub fn bdp(&self) -> u64 {
        (self.max_bw * self.min_rtt) as u64
    }

    /// inflight limit: BDP * gain 기반 전송량 제한
    pub fn inflight_limit(&self) -> u64 {
        let gain = match self.state {
            BbrState::Startup => 3.0,
            BbrState::ProbeUp => 2.0,
            BbrState::ProbeDrain => 1.0,
            BbrState::Cruise => 1.5,
        };
        (self.bdp() as f64 * gain) as u64
    }

    /// 손실률 기반 동적 redundancy 비율 계산
    /// NACK가 빠지는 청크를 복구하므로 redundancy는 보수적으로 유지
    /// - loss < 0.1%: min_redundancy (대역폭 확보)
    /// - loss 0.1~5%: loss_rate * 1.5 (손실률에 비례, base 미적용)
    /// - loss 5~15%: loss_rate * 1.2 (NACK 보완)
    /// - loss > 15%: max_redundancy
    pub fn recommended_redundancy(&self, _base: f64, min: f64, max: f64) -> f64 {
        // 수신자 피드백 없으면 min redundancy (피드백 전에 과다 중복 방지)
        if self.receiver_delivery_rate <= 0.0 {
            return min;
        }
        if self.loss_rate < 0.001 {
            return min;
        }

        // 손실률에 비례한 redundancy (base를 더하지 않음 — NACK가 나머지 복구)
        let ratio = if self.loss_rate > 0.15 {
            max
        } else if self.loss_rate > 0.05 {
            self.loss_rate * 1.2
        } else {
            self.loss_rate * 1.5
        };

        ratio.clamp(min, max)
    }
}
