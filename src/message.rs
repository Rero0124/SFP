//! 프로토콜 메시지 정의
//!
//! NACK 기반 프로토콜이므로 메시지는 최소화됨

use serde::{Deserialize, Serialize};

use crate::{ChunkId, SegmentId, MAGIC_NUMBER, PROTOCOL_VERSION};

/// 메시지 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    /// 데이터 청크
    Chunk = 1,

    /// NACK (누락 청크 요청)
    Nack = 2,

    /// 세그먼트 완료 알림
    SegmentComplete = 3,

    /// 연결 초기화
    Init = 4,

    /// 연결 초기화 응답
    InitAck = 5,

    /// 연결 종료
    Close = 6,

    /// Heartbeat (생존 확인)
    Heartbeat = 7,

    /// Heartbeat 응답
    HeartbeatAck = 8,

    /// 통계 보고 (선택적)
    Stats = 9,

    /// 흐름 제어 피드백 (클라이언트 → 서버)
    FlowControl = 10,

    /// 전송 완료 신호 (서버 → 클라이언트)
    TransmissionComplete = 11,

    /// 전송 진행 상황 (서버 → 클라이언트, 정확한 loss 계산용)
    SendProgress = 12,

}

/// 메시지 헤더
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeader {
    /// 매직 넘버
    pub magic: u32,

    /// 프로토콜 버전
    pub version: u8,

    /// 메시지 타입
    pub msg_type: MessageType,

    /// 메시지 길이 (헤더 제외)
    pub payload_len: u32,
}

impl MessageHeader {
    pub fn new(msg_type: MessageType, payload_len: u32) -> Self {
        Self {
            magic: MAGIC_NUMBER,
            version: PROTOCOL_VERSION,
            msg_type,
            payload_len,
        }
    }
}

/// NACK 메시지 (누락 청크 요청)
///
/// 클라이언트에서 서버로 보내는 유일한 주요 메시지
/// 크기를 최소화하여 업링크 부담 줄임
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NackMessage {
    /// 세그먼트 ID
    pub segment_id: SegmentId,

    /// 누락된 청크 ID 목록
    /// 비트맵이나 범위로 압축 가능하지만 단순 리스트로 시작
    pub missing_chunk_ids: Vec<ChunkId>,

    /// 현재 수신률 (통계용)
    pub receive_ratio: f32,

    /// NIC ID (어느 경로로 재전송 요청인지)
    pub nic_id: u8,
}

impl NackMessage {
    pub fn new(
        segment_id: SegmentId,
        missing_chunk_ids: Vec<ChunkId>,
        receive_ratio: f32,
        nic_id: u8,
    ) -> Self {
        Self {
            segment_id,
            missing_chunk_ids,
            receive_ratio,
            nic_id,
        }
    }

    /// 바이트로 직렬화 (최소 크기)
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::Nack, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    /// 바이트에서 역직렬화
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        // bincode는 가변 길이이므로 직접 역직렬화 시도
        // 헤더: magic(4) + version(1) + msg_type(1) + payload_len(4) = 약 14~20바이트
        if bytes.len() < 14 {
            return None;
        }

        // 헤더 파싱 시도 (bincode는 앞에서부터 읽음)
        let header: MessageHeader = match bincode::deserialize(bytes) {
            Ok(h) => h,
            Err(_) => return None,
        };

        if header.msg_type != MessageType::Nack {
            return None;
        }

        // 헤더 직렬화해서 실제 크기 확인
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();

        if bytes.len() < header_size {
            return None;
        }

        // 페이로드 파싱
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 세그먼트 완료 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentCompleteMessage {
    pub segment_id: SegmentId,
    pub total_chunks_received: u32,
    pub duplicates_received: u32,
    pub elapsed_ms: u64,
}

impl SegmentCompleteMessage {
    pub fn new(segment_id: SegmentId, total_chunks_received: u32, elapsed_ms: u64) -> Self {
        Self {
            segment_id,
            total_chunks_received,
            duplicates_received: 0,
            elapsed_ms,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::SegmentComplete, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 {
            return None;
        }
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::SegmentComplete {
            return None;
        }
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        if bytes.len() < header_size {
            return None;
        }
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 연결 초기화 메시지 (클라이언트 → 서버)
///
/// 클라이언트가 서버에 연결 시 보내는 초기 핸드쉐이크 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitMessage {
    /// 클라이언트 공개키 (X25519, 32바이트)
    /// 암호화 비활성 시 0으로 채움
    pub client_public_key: [u8; 32],
    
    /// 암호화 활성화 요청 여부
    pub encryption_enabled: bool,

    /// 클라이언트가 지원하는 NIC 수
    pub nic_count: u8,

    /// 요청 청크 크기 (0이면 서버 기본값 사용)
    pub chunk_size: u16,

    /// 요청 세그먼트 크기 (0이면 서버 기본값 사용)
    pub segment_size: u32,

    /// 버퍼 크기 힌트 (바이트)
    pub buffer_size: u32,
    
    /// 프로토콜 버전
    pub protocol_version: u8,
    
    /// 클라이언트 타임스탬프 (microseconds since epoch) - RTT 측정용
    pub timestamp_us: u64,
}

impl InitMessage {
    pub fn new(encryption_enabled: bool, client_public_key: [u8; 32]) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        
        Self {
            client_public_key,
            encryption_enabled,
            nic_count: 1,
            chunk_size: 0, // 서버 기본값 사용
            segment_size: 0, // 서버 기본값 사용
            buffer_size: 2 * 1024 * 1024,
            protocol_version: crate::PROTOCOL_VERSION,
            timestamp_us,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::Init, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 {
            return None;
        }
        
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::Init {
            return None;
        }
        
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        
        if bytes.len() < header_size {
            return None;
        }
        
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 연결 초기화 응답 (서버 → 클라이언트)
///
/// 서버가 클라이언트의 Init에 응답하여 보내는 메시지
/// 이 메시지를 받으면 클라이언트는 데이터 수신 준비 완료
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitAckMessage {
    /// 서버 공개키 (X25519, 32바이트)
    /// 암호화 비활성 시 0으로 채움
    pub server_public_key: [u8; 32],
    
    /// 세션 키 (ChaCha20-Poly1305, 32바이트)
    /// 암호화 비활성 시 0으로 채움
    /// 실제 구현에서는 ECDH로 유도해야 함
    pub session_key: [u8; 32],
    
    /// 암호화 활성화 여부
    pub encryption_enabled: bool,

    /// 서버가 결정한 NIC 수
    pub nic_count: u8,

    /// 확정된 청크 크기
    pub chunk_size: u16,

    /// 확정된 세그먼트 크기
    pub segment_size: u32,

    /// 서버 기본 중복률
    pub redundancy_ratio: f32,
    
    /// 전송할 총 파일 크기 (바이트)
    pub total_file_size: u64,
    
    /// 총 세그먼트 수
    pub total_segments: u64,
    
    /// 세그먼트당 청크 수
    pub chunks_per_segment: u32,
    
    /// 프로토콜 버전
    pub protocol_version: u8,
    
    /// 클라이언트 타임스탬프 에코 (클라이언트가 보낸 값 그대로 반환)
    pub client_timestamp_us: u64,
    
    /// 서버 타임스탬프 (서버에서 응답 보낼 때 시간)
    pub server_timestamp_us: u64,
}

impl InitAckMessage {
    pub fn new(
        total_file_size: u64,
        chunk_size: u16,
        segment_size: u32,
        redundancy_ratio: f32,
    ) -> Self {
        Self::with_client_timestamp(total_file_size, chunk_size, segment_size, redundancy_ratio, 0)
    }
    
    /// 클라이언트 타임스탬프를 포함한 생성자 (RTT 측정용)
    pub fn with_client_timestamp(
        total_file_size: u64,
        chunk_size: u16,
        segment_size: u32,
        redundancy_ratio: f32,
        client_timestamp_us: u64,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let server_timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        
        let chunks_per_segment = ((segment_size as usize + chunk_size as usize - 1) / chunk_size as usize) as u32;
        let total_segments = (total_file_size + segment_size as u64 - 1) / segment_size as u64;
        
        Self {
            server_public_key: [0u8; 32],
            session_key: [0u8; 32],
            encryption_enabled: false,
            nic_count: 1,
            chunk_size,
            segment_size,
            redundancy_ratio,
            total_file_size,
            total_segments,
            chunks_per_segment,
            protocol_version: crate::PROTOCOL_VERSION,
            client_timestamp_us,
            server_timestamp_us,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::InitAck, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 {
            return None;
        }
        
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::InitAck {
            return None;
        }
        
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        
        if bytes.len() < header_size {
            return None;
        }
        
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// Heartbeat 메시지
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub sequence: u64,
    pub timestamp_us: u64,
}

impl HeartbeatMessage {
    pub fn new(sequence: u64) -> Self {
        let timestamp_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        Self {
            sequence,
            timestamp_us,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::Heartbeat, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }
}

/// 흐름 제어 메시지 (클라이언트 → 서버)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlMessage {
    /// 수신 버퍼 여유 공간 (세그먼트 단위)
    pub buffer_available: u32,
    /// 마지막 완료된 세그먼트 ID
    pub last_completed_segment: u64,
    /// 현재 처리 중인 세그먼트 수
    pub segments_in_progress: u32,
    /// 최근 패킷 손실률 (0.0 ~ 1.0)
    pub loss_rate: f32,
    /// 현재 처리 속도 (세그먼트/초)
    pub processing_rate: f32,
    /// 권장 전송 속도 (세그먼트/초, 0이면 서버 판단)
    pub suggested_rate: f32,
}

impl FlowControlMessage {
    pub fn new(
        buffer_available: u32,
        last_completed_segment: u64,
        segments_in_progress: u32,
        loss_rate: f32,
        processing_rate: f32,
    ) -> Self {
        // 손실률과 처리 속도 기반으로 권장 속도 계산
        let suggested_rate = if loss_rate > 0.1 {
            // 손실률 10% 이상이면 속도 절반
            processing_rate * 0.5
        } else if loss_rate > 0.05 {
            // 손실률 5% 이상이면 속도 유지
            processing_rate
        } else if buffer_available > 100 {
            // 버퍼 여유 있고 손실률 낮으면 속도 증가
            processing_rate * 1.2
        } else {
            processing_rate
        };

        Self {
            buffer_available,
            last_completed_segment,
            segments_in_progress,
            loss_rate,
            processing_rate,
            suggested_rate,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::FlowControl, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::FlowControl {
            return None;
        }
        
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        
        if bytes.len() < header_size {
            return None;
        }
        
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 전송 진행 메시지 (서버 → 클라이언트)
///
/// 서버가 주기적으로 보내어 클라이언트가 정확한 loss rate를 계산할 수 있게 함
/// loss = (total_chunks_sent - client_received) / total_chunks_sent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendProgressMessage {
    /// 서버가 보낸 총 청크 수 (중복 포함)
    pub total_chunks_sent: u64,
    /// 현재까지 전송한 세그먼트 수
    pub segments_sent: u64,
}

impl SendProgressMessage {
    pub fn new(total_chunks_sent: u64, segments_sent: u64) -> Self {
        Self { total_chunks_sent, segments_sent }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::SendProgress, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 {
            return None;
        }
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::SendProgress {
            return None;
        }
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        if bytes.len() < header_size {
            return None;
        }
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 전송 완료 메시지 (서버 → 클라이언트)
///
/// 서버가 모든 세그먼트의 1차 전송을 완료했음을 알림
/// 클라이언트는 이 메시지 수신 후부터 NACK를 시작
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransmissionCompleteMessage {
    /// 전송한 총 세그먼트 수
    pub total_segments: u64,
    /// 전송한 총 청크 수 (중복 포함)
    pub total_chunks_sent: u64,
    /// 타임스탬프
    pub timestamp_us: u64,
}

impl TransmissionCompleteMessage {
    pub fn new(total_segments: u64, total_chunks_sent: u64) -> Self {
        let timestamp_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self {
            total_segments,
            total_chunks_sent,
            timestamp_us,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).unwrap_or_default();
        let header = MessageHeader::new(MessageType::TransmissionComplete, payload.len() as u32);
        let header_bytes = bincode::serialize(&header).unwrap_or_default();

        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&payload);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 10 {
            return None;
        }
        let header: MessageHeader = bincode::deserialize(bytes).ok()?;
        if header.msg_type != MessageType::TransmissionComplete {
            return None;
        }
        let header_bytes = bincode::serialize(&header).ok()?;
        let header_size = header_bytes.len();
        if bytes.len() < header_size {
            return None;
        }
        bincode::deserialize(&bytes[header_size..]).ok()
    }
}

/// 통합 메시지 enum
#[derive(Debug, Clone)]
pub enum Message {
    Nack(NackMessage),
    SegmentComplete(SegmentCompleteMessage),
    Init(InitMessage),
    InitAck(InitAckMessage),
    Heartbeat(HeartbeatMessage),
    FlowControl(FlowControlMessage),
    TransmissionComplete(TransmissionCompleteMessage),
    SendProgress(SendProgressMessage),
    Close,
}

impl Message {
    /// 메시지 타입 반환
    pub fn msg_type(&self) -> MessageType {
        match self {
            Message::Nack(_) => MessageType::Nack,
            Message::SegmentComplete(_) => MessageType::SegmentComplete,
            Message::Init(_) => MessageType::Init,
            Message::InitAck(_) => MessageType::InitAck,
            Message::Heartbeat(_) => MessageType::Heartbeat,
            Message::FlowControl(_) => MessageType::FlowControl,
            Message::TransmissionComplete(_) => MessageType::TransmissionComplete,
            Message::SendProgress(_) => MessageType::SendProgress,
            Message::Close => MessageType::Close,
        }
    }
}
