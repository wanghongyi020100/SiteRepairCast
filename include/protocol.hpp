#pragma once

#include<arpa/inet.h>

#include<algorithm>
#include<array>
#include<cstddef>
#include<cstdint>
#include<cstring>
#include<limits>
#include<stdexcept>
#include<utility>
#include<vector>

namespace srcast
{
constexpr std::uint32_t kMagic=0x53524331U;
constexpr std::uint32_t kControlMagic=0x53524343U;
constexpr std::uint16_t kVersion=5;
constexpr std::size_t kPayloadSize=1200;
constexpr std::size_t kSha256Size=32;
constexpr std::uint32_t kSingleSectionId=0;
constexpr std::uint32_t kSectionBlockCount=64;

constexpr std::size_t kCommonHeaderSize=16;
constexpr std::size_t kMetaPacketSize=
    kCommonHeaderSize+8+4+4+kSha256Size;
constexpr std::size_t kDataHeaderSize=
    kCommonHeaderSize+4+4+8+2+2+4;
constexpr std::size_t kEndPacketSize=kCommonHeaderSize+4+4+4;
constexpr std::size_t kMaxPacketSize=kDataHeaderSize+kPayloadSize;
constexpr std::size_t kMaxControlFrameSize=1024 * 1024;

inline std::uint64_t host_to_network64(std::uint64_t value)
{
#if __BYTE_ORDER__==__ORDER_LITTLE_ENDIAN__
    return
        (static_cast<std::uint64_t>(
             htonl(static_cast<std::uint32_t>(value & 0xffffffffULL)))
<<32U)|
        htonl(static_cast<std::uint32_t>(value>>32U));
#else
    return value;
#endif
}

inline std::uint64_t network_to_host64(std::uint64_t value)
{
    return host_to_network64(value);
}

class PacketWriter
{
public:
    explicit PacketWriter(std::size_t reserve=0) { data_.reserve(reserve); }

    void u16(std::uint16_t value)
    {
        value=htons(value);
        bytes(&value,sizeof(value));
    }

    void u32(std::uint32_t value)
    {
        value=htonl(value);
        bytes(&value,sizeof(value));
    }

    void u64(std::uint64_t value)
    {
        value=host_to_network64(value);
        bytes(&value,sizeof(value));
    }

    void bytes(const void*source,std::size_t size)
    {
        if(size==0)
        {
            return;
        }
        const auto*begin=static_cast<const std::uint8_t*>(source);
        data_.insert(data_.end(),begin,begin+size);
    }

    [[nodiscard]] const std::vector<std::uint8_t>&data()const { return data_; }

private:
    std::vector<std::uint8_t>data_;
};

class PacketReader
{
public:
    PacketReader(const void*data,std::size_t size)
        : data_(static_cast<const std::uint8_t*>(data)),size_(size) {}

    std::uint16_t u16()
    {
        std::uint16_t value{};
        read(&value,sizeof(value));
        return ntohs(value);
    }

    std::uint32_t u32()
    {
        std::uint32_t value{};
        read(&value,sizeof(value));
        return ntohl(value);
    }

    std::uint64_t u64()
    {
        std::uint64_t value{};
        read(&value,sizeof(value));
        return network_to_host64(value);
    }

    void bytes(void*destination,std::size_t size)
    {
        if(size==0)
        {
            return;
        }
        read(destination,size);
    }

    [[nodiscard]] std::size_t remaining()const { return size_-offset_; }

private:
    void read(void*destination,std::size_t size)
    {
        if(size>remaining())
        {
            throw std::runtime_error("truncated packet");
        }
        std::memcpy(destination,data_+offset_,size);
        offset_+=size;
    }

    const std::uint8_t*data_;
    std::size_t size_;
    std::size_t offset_{0};
};

enum class PacketType : std::uint16_t
{
    Meta=1,
    Data=2,
    End=3,
};

struct CommonHeader
{
    PacketType type{};
    std::uint64_t transfer_id{};
};

struct MetaPacket
{
    CommonHeader common;
    std::uint64_t file_size{};
    std::uint32_t block_size{};
    std::uint32_t total_blocks{};
    std::array<std::uint8_t,kSha256Size>sha256{};
};

struct DataPacketView
{
    CommonHeader common;
    std::uint32_t section_id{};
    std::uint32_t block_id{};
    std::uint64_t offset{};
    std::uint16_t payload_size{};
    std::uint32_t crc32{};
    const std::uint8_t*payload{};
};

struct EndPacket
{
    CommonHeader common;
    std::uint32_t section_id{};
    std::uint32_t round_id{};
    std::uint32_t total_blocks{};
};

inline void write_common(
    PacketWriter&writer,
    PacketType type,
    std::uint64_t transfer_id)
    {

    writer.u32(kMagic);
    writer.u16(kVersion);
    writer.u16(static_cast<std::uint16_t>(type));
    writer.u64(transfer_id);
}

inline CommonHeader read_common(PacketReader&reader)
{
    const auto magic=reader.u32();
    const auto version=reader.u16();
    const auto raw_type=reader.u16();
    const auto transfer_id=reader.u64();

    if(magic!=kMagic)
    {
        throw std::runtime_error("invalid packet magic");
    }
    if(version!=kVersion)
    {
        throw std::runtime_error("unsupported protocol version");
    }
    if(raw_type<static_cast<std::uint16_t>(PacketType::Meta)||
        raw_type>static_cast<std::uint16_t>(PacketType::End))
        {
        throw std::runtime_error("invalid packet type");
    }
    return {static_cast<PacketType>(raw_type),transfer_id};
}

inline std::vector<std::uint8_t>encode_meta(const MetaPacket&packet)
{
    PacketWriter writer(kMetaPacketSize);
    write_common(writer,PacketType::Meta,packet.common.transfer_id);
    writer.u64(packet.file_size);
    writer.u32(packet.block_size);
    writer.u32(packet.total_blocks);
    writer.bytes(packet.sha256.data(),packet.sha256.size());
    return writer.data();
}

inline MetaPacket decode_meta(const void*data,std::size_t size)
{
    if(size!=kMetaPacketSize)
    {
        throw std::runtime_error("invalid META packet size");
    }
    PacketReader reader(data,size);
    MetaPacket packet;
    packet.common=read_common(reader);
    if(packet.common.type!=PacketType::Meta)
    {
        throw std::runtime_error("packet is not META");
    }
    packet.file_size=reader.u64();
    packet.block_size=reader.u32();
    packet.total_blocks=reader.u32();
    reader.bytes(packet.sha256.data(),packet.sha256.size());
    return packet;
}

inline std::vector<std::uint8_t>encode_data(
    std::uint64_t transfer_id,
    std::uint32_t section_id,
    std::uint32_t block_id,
    std::uint64_t offset,
    const std::uint8_t*payload,
    std::uint16_t payload_size,
    std::uint32_t crc32)
    {

    if(payload_size>kPayloadSize)
    {
        throw std::runtime_error("DATA payload is too large");
    }

    PacketWriter writer(kDataHeaderSize+payload_size);
    write_common(writer,PacketType::Data,transfer_id);
    writer.u32(section_id);
    writer.u32(block_id);
    writer.u64(offset);
    writer.u16(payload_size);
    writer.u16(0);
    writer.u32(crc32);
    writer.bytes(payload,payload_size);
    return writer.data();
}

inline DataPacketView decode_data(const void*data,std::size_t size)
{
    if(size<kDataHeaderSize)
    {
        throw std::runtime_error("DATA packet is too short");
    }
    PacketReader reader(data,size);
    DataPacketView packet;
    packet.common=read_common(reader);
    if(packet.common.type!=PacketType::Data)
    {
        throw std::runtime_error("packet is not DATA");
    }
    packet.section_id=reader.u32();
    packet.block_id=reader.u32();
    packet.offset=reader.u64();
    packet.payload_size=reader.u16();
    static_cast<void>(reader.u16());
    packet.crc32=reader.u32();
    if(packet.payload_size>kPayloadSize||
        packet.payload_size!=reader.remaining())
        {
        throw std::runtime_error("invalid DATA payload size");
    }
    packet.payload=static_cast<const std::uint8_t*>(data)+kDataHeaderSize;
    return packet;
}

inline std::vector<std::uint8_t>encode_end(
    std::uint64_t transfer_id,
    std::uint32_t section_id,
    std::uint32_t round_id,
    std::uint32_t total_blocks)
    {

    PacketWriter writer(kEndPacketSize);
    write_common(writer,PacketType::End,transfer_id);
    writer.u32(section_id);
    writer.u32(round_id);
    writer.u32(total_blocks);
    return writer.data();
}

inline EndPacket decode_end(const void*data,std::size_t size)
{
    if(size!=kEndPacketSize)
    {
        throw std::runtime_error("invalid END packet size");
    }
    PacketReader reader(data,size);
    EndPacket packet;
    packet.common=read_common(reader);
    if(packet.common.type!=PacketType::End)
    {
        throw std::runtime_error("packet is not END");
    }
    packet.section_id=reader.u32();
    packet.round_id=reader.u32();
    packet.total_blocks=reader.u32();
    return packet;
}

enum class ControlType : std::uint16_t
{
    Register=1,
    SectionStatus=2,
    RepairBegin=3,
    ReceiverComplete=4,
    CompleteAck=5,
    TransferResult=6,
    SectionEnd=7,
    ReceiverReady=8,
    SessionEnd=9,
    FileMeta=10,
    MetaReady=11,
    CentralFileMeta=12,
    CentralData=13,
    CentralFileEnd=14,
    CentralStatus=15,
    CentralSessionEnd=16,
};

enum class SectionStatusCode : std::uint16_t
{
    Missing=1,
    Complete=2,
    Failed=3,
};

enum class TransferResultCode : std::uint16_t
{
    Completed=1,
    Failed=2,
};

enum class CentralStatusCode : std::uint16_t
{
    Cached=1,
    Failed=2,
};

struct RegisterMessage
{
    std::uint64_t receiver_id{};
    std::uint16_t udp_port{};
};

struct FileMetaMessage
{
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint64_t file_size{};
    std::uint32_t block_size{};
    std::uint32_t total_blocks{};
    std::array<std::uint8_t,kSha256Size>sha256{};
};

struct MetaReadyMessage
{
    std::uint64_t receiver_id{};
    std::uint64_t transfer_id{};
};

struct SectionStatusMessage
{
    std::uint64_t receiver_id{};
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint32_t round_id{};
    SectionStatusCode status{};
    std::uint32_t missing_count{};
    std::vector<std::uint8_t>missing_bitmap;
};

struct RepairBeginMessage
{
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint32_t round_id{};
};

struct ReceiverCompleteMessage
{
    std::uint64_t receiver_id{};
    std::uint64_t transfer_id{};
    std::uint64_t file_size{};
    std::array<std::uint8_t,kSha256Size>sha256{};
};

struct CompleteAckMessage
{
    std::uint64_t receiver_id{};
    std::uint64_t transfer_id{};
};

struct TransferResultMessage
{
    std::uint64_t transfer_id{};
    TransferResultCode result{};
};

struct SectionEndMessage
{
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint32_t round_id{};
    std::uint32_t total_blocks{};
};

struct ReceiverReadyMessage
{
    std::uint64_t receiver_id{};
    std::uint64_t previous_transfer_id{};
};

struct CentralFileMetaMessage
{
    std::uint64_t transfer_id{};
    std::uint64_t file_size{};
    std::uint32_t block_size{};
    std::uint32_t total_blocks{};
    std::array<std::uint8_t,kSha256Size>sha256{};
};

struct CentralDataMessage
{
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint32_t block_id{};
    std::uint64_t offset{};
    std::uint16_t payload_size{};
    std::uint32_t crc32{};
    std::vector<std::uint8_t>payload;
};

struct CentralFileEndMessage
{
    std::uint64_t transfer_id{};
    std::uint32_t section_id{};
    std::uint32_t total_blocks{};
};

struct CentralStatusMessage
{
    std::uint64_t transfer_id{};
    CentralStatusCode status{};
    std::uint64_t file_size{};
    std::array<std::uint8_t,kSha256Size>sha256{};
};

inline void write_control_header(PacketWriter&writer,ControlType type)
{
    writer.u32(kControlMagic);
    writer.u16(kVersion);
    writer.u16(static_cast<std::uint16_t>(type));
}

inline ControlType read_control_header(PacketReader&reader)
{
    const auto magic=reader.u32();
    const auto version=reader.u16();
    const auto raw_type=reader.u16();

    if(magic!=kControlMagic)
    {
        throw std::runtime_error("invalid control magic");
    }
    if(version!=kVersion)
    {
        throw std::runtime_error("unsupported control protocol version");
    }
    if(raw_type<static_cast<std::uint16_t>(ControlType::Register)||
        raw_type>static_cast<std::uint16_t>(ControlType::CentralSessionEnd))
        {
        throw std::runtime_error("invalid control message type");
    }
    return static_cast<ControlType>(raw_type);
}

inline ControlType peek_control_type(const std::vector<std::uint8_t>&frame)
{
    PacketReader reader(frame.data(),frame.size());
    return read_control_header(reader);
}

inline std::vector<std::uint8_t>encode_register(const RegisterMessage&message)
{
    PacketWriter writer(20);
    write_control_header(writer,ControlType::Register);
    writer.u64(message.receiver_id);
    writer.u16(message.udp_port);
    writer.u16(0);
    return writer.data();
}

inline RegisterMessage decode_register(const std::vector<std::uint8_t>&frame)
{
    if(frame.size()!=20)
    {
        throw std::runtime_error("invalid REGISTER size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::Register)
    {
        throw std::runtime_error("control message is not REGISTER");
    }
    RegisterMessage message;
    message.receiver_id=reader.u64();
    message.udp_port=reader.u16();
    static_cast<void>(reader.u16());
    return message;
}

inline std::vector<std::uint8_t>encode_file_meta(
    const FileMetaMessage&message)
    {













    PacketWriter writer(68);

    write_control_header(
        writer,
        ControlType::FileMeta);

    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u64(message.file_size);
    writer.u32(message.block_size);
    writer.u32(message.total_blocks);

    writer.bytes(
        message.sha256.data(),
        message.sha256.size());

    return writer.data();
}

inline FileMetaMessage decode_file_meta(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=68)
    {
        throw std::runtime_error(
            "invalid FILE_META size");
    }

    PacketReader reader(
        frame.data(),
        frame.size());

    if(read_control_header(reader)!=
        ControlType::FileMeta)
        {
        throw std::runtime_error(
            "control message is not FILE_META");
    }

    FileMetaMessage message;

    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.file_size=reader.u64();
    message.block_size=reader.u32();
    message.total_blocks=reader.u32();

    reader.bytes(
        message.sha256.data(),
        message.sha256.size());

    return message;
}

inline std::vector<std::uint8_t>encode_meta_ready(
    const MetaReadyMessage&message)
    {



    PacketWriter writer(24);

    write_control_header(
        writer,
        ControlType::MetaReady);

    writer.u64(message.receiver_id);
    writer.u64(message.transfer_id);

    return writer.data();
}

inline MetaReadyMessage decode_meta_ready(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=24)
    {
        throw std::runtime_error(
            "invalid META_READY size");
    }

    PacketReader reader(
        frame.data(),
        frame.size());

    if(read_control_header(reader)!=
        ControlType::MetaReady)
        {
        throw std::runtime_error(
            "control message is not META_READY");
    }

    MetaReadyMessage message;

    message.receiver_id=reader.u64();
    message.transfer_id=reader.u64();

    return message;
}

inline std::vector<std::uint8_t>encode_section_status(
    const SectionStatusMessage&message)
    {

    if(message.missing_bitmap.size()>
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()))
        {
        throw std::runtime_error("missing bitmap is too large");
    }

    PacketWriter writer(44+message.missing_bitmap.size());
    write_control_header(writer,ControlType::SectionStatus);
    writer.u64(message.receiver_id);
    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u32(message.round_id);
    writer.u16(static_cast<std::uint16_t>(message.status));
    writer.u16(0);
    writer.u32(message.missing_count);
    writer.u32(static_cast<std::uint32_t>(message.missing_bitmap.size()));
    writer.bytes(message.missing_bitmap.data(),message.missing_bitmap.size());
    return writer.data();
}

inline SectionStatusMessage decode_section_status(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()<44)
    {
        throw std::runtime_error("SECTION_STATUS is too short");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::SectionStatus)
    {
        throw std::runtime_error("control message is not SECTION_STATUS");
    }

    SectionStatusMessage message;
    message.receiver_id=reader.u64();
    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.round_id=reader.u32();
    const auto raw_status=reader.u16();
    static_cast<void>(reader.u16());
    message.missing_count=reader.u32();
    const auto bitmap_size=reader.u32();

    if(raw_status<static_cast<std::uint16_t>(SectionStatusCode::Missing)||
        raw_status>static_cast<std::uint16_t>(SectionStatusCode::Failed))
        {
        throw std::runtime_error("invalid SECTION_STATUS code");
    }
    if(bitmap_size!=reader.remaining())
    {
        throw std::runtime_error("invalid SECTION_STATUS bitmap length");
    }

    message.status=static_cast<SectionStatusCode>(raw_status);
    message.missing_bitmap.resize(bitmap_size);
    reader.bytes(message.missing_bitmap.data(),message.missing_bitmap.size());
    return message;
}

inline std::vector<std::uint8_t>encode_repair_begin(
    const RepairBeginMessage&message)
    {

    PacketWriter writer(24);
    write_control_header(writer,ControlType::RepairBegin);
    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u32(message.round_id);
    return writer.data();
}

inline RepairBeginMessage decode_repair_begin(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=24)
    {
        throw std::runtime_error("invalid REPAIR_BEGIN size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::RepairBegin)
    {
        throw std::runtime_error("control message is not REPAIR_BEGIN");
    }
    RepairBeginMessage message;
    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.round_id=reader.u32();
    return message;
}

inline std::vector<std::uint8_t>encode_receiver_complete(
    const ReceiverCompleteMessage&message)
    {

    PacketWriter writer(64);
    write_control_header(writer,ControlType::ReceiverComplete);
    writer.u64(message.receiver_id);
    writer.u64(message.transfer_id);
    writer.u64(message.file_size);
    writer.bytes(message.sha256.data(),message.sha256.size());
    return writer.data();
}

inline ReceiverCompleteMessage decode_receiver_complete(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=64)
    {
        throw std::runtime_error("invalid RECEIVER_COMPLETE size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::ReceiverComplete)
    {
        throw std::runtime_error("control message is not RECEIVER_COMPLETE");
    }
    ReceiverCompleteMessage message;
    message.receiver_id=reader.u64();
    message.transfer_id=reader.u64();
    message.file_size=reader.u64();
    reader.bytes(message.sha256.data(),message.sha256.size());
    return message;
}

inline std::vector<std::uint8_t>encode_complete_ack(
    const CompleteAckMessage&message)
    {

    PacketWriter writer(24);
    write_control_header(writer,ControlType::CompleteAck);
    writer.u64(message.receiver_id);
    writer.u64(message.transfer_id);
    return writer.data();
}

inline CompleteAckMessage decode_complete_ack(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=24)
    {
        throw std::runtime_error("invalid COMPLETE_ACK size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CompleteAck)
    {
        throw std::runtime_error("control message is not COMPLETE_ACK");
    }
    CompleteAckMessage message;
    message.receiver_id=reader.u64();
    message.transfer_id=reader.u64();
    return message;
}

inline std::vector<std::uint8_t>encode_transfer_result(
    const TransferResultMessage&message)
    {

    PacketWriter writer(20);
    write_control_header(writer,ControlType::TransferResult);
    writer.u64(message.transfer_id);
    writer.u16(static_cast<std::uint16_t>(message.result));
    writer.u16(0);
    return writer.data();
}

inline TransferResultMessage decode_transfer_result(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=20)
    {
        throw std::runtime_error("invalid TRANSFER_RESULT size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::TransferResult)
    {
        throw std::runtime_error("control message is not TRANSFER_RESULT");
    }
    TransferResultMessage message;
    message.transfer_id=reader.u64();
    const auto raw_result=reader.u16();
    static_cast<void>(reader.u16());
    if(raw_result<static_cast<std::uint16_t>(TransferResultCode::Completed)||
        raw_result>static_cast<std::uint16_t>(TransferResultCode::Failed))
        {
        throw std::runtime_error("invalid TRANSFER_RESULT code");
    }
    message.result=static_cast<TransferResultCode>(raw_result);
    return message;
}

inline std::vector<std::uint8_t>encode_section_end(
    const SectionEndMessage&message)
    {

    PacketWriter writer(28);
    write_control_header(writer,ControlType::SectionEnd);
    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u32(message.round_id);
    writer.u32(message.total_blocks);
    return writer.data();
}

inline SectionEndMessage decode_section_end(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=28)
    {
        throw std::runtime_error("invalid SECTION_END size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::SectionEnd)
    {
        throw std::runtime_error("control message is not SECTION_END");
    }
    SectionEndMessage message;
    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.round_id=reader.u32();
    message.total_blocks=reader.u32();
    return message;
}

inline std::vector<std::uint8_t>encode_receiver_ready(
    const ReceiverReadyMessage&message)
    {

    PacketWriter writer(24);
    write_control_header(writer,ControlType::ReceiverReady);
    writer.u64(message.receiver_id);
    writer.u64(message.previous_transfer_id);
    return writer.data();
}

inline ReceiverReadyMessage decode_receiver_ready(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=24)
    {
        throw std::runtime_error("invalid RECEIVER_READY size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::ReceiverReady)
    {
        throw std::runtime_error("control message is not RECEIVER_READY");
    }
    ReceiverReadyMessage message;
    message.receiver_id=reader.u64();
    message.previous_transfer_id=reader.u64();
    return message;
}

inline std::vector<std::uint8_t>encode_session_end()
{
    PacketWriter writer(8);
    write_control_header(writer,ControlType::SessionEnd);
    return writer.data();
}

inline void decode_session_end(const std::vector<std::uint8_t>&frame)
{
    if(frame.size()!=8)
    {
        throw std::runtime_error("invalid SESSION_END size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::SessionEnd)
    {
        throw std::runtime_error("control message is not SESSION_END");
    }
}

inline std::vector<std::uint8_t>encode_central_file_meta(
    const CentralFileMetaMessage&message)
    {

    PacketWriter writer(64);
    write_control_header(writer,ControlType::CentralFileMeta);
    writer.u64(message.transfer_id);
    writer.u64(message.file_size);
    writer.u32(message.block_size);
    writer.u32(message.total_blocks);
    writer.bytes(message.sha256.data(),message.sha256.size());
    return writer.data();
}

inline CentralFileMetaMessage decode_central_file_meta(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=64)
    {
        throw std::runtime_error("invalid CENTRAL_FILE_META size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CentralFileMeta)
    {
        throw std::runtime_error("control message is not CENTRAL_FILE_META");
    }

    CentralFileMetaMessage message;
    message.transfer_id=reader.u64();
    message.file_size=reader.u64();
    message.block_size=reader.u32();
    message.total_blocks=reader.u32();
    reader.bytes(message.sha256.data(),message.sha256.size());
    return message;
}

inline std::vector<std::uint8_t>encode_central_data(
    const CentralDataMessage&message)
    {

    if(message.payload.size()>kPayloadSize||
        message.payload.size()!=message.payload_size)
        {
        throw std::runtime_error("invalid CENTRAL_DATA payload size");
    }

    PacketWriter writer(40+message.payload.size());
    write_control_header(writer,ControlType::CentralData);
    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u32(message.block_id);
    writer.u64(message.offset);
    writer.u16(message.payload_size);
    writer.u16(0);
    writer.u32(message.crc32);
    writer.bytes(message.payload.data(),message.payload.size());
    return writer.data();
}

inline CentralDataMessage decode_central_data(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()<40)
    {
        throw std::runtime_error("CENTRAL_DATA is too short");
    }

    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CentralData)
    {
        throw std::runtime_error("control message is not CENTRAL_DATA");
    }

    CentralDataMessage message;
    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.block_id=reader.u32();
    message.offset=reader.u64();
    message.payload_size=reader.u16();
    static_cast<void>(reader.u16());
    message.crc32=reader.u32();

    if(message.payload_size>kPayloadSize||
        message.payload_size!=reader.remaining())
        {
        throw std::runtime_error("invalid CENTRAL_DATA payload length");
    }

    message.payload.resize(message.payload_size);
    reader.bytes(message.payload.data(),message.payload.size());
    return message;
}

inline std::vector<std::uint8_t>encode_central_file_end(
    const CentralFileEndMessage&message)
    {

    PacketWriter writer(24);
    write_control_header(writer,ControlType::CentralFileEnd);
    writer.u64(message.transfer_id);
    writer.u32(message.section_id);
    writer.u32(message.total_blocks);
    return writer.data();
}

inline CentralFileEndMessage decode_central_file_end(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=24)
    {
        throw std::runtime_error("invalid CENTRAL_FILE_END size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CentralFileEnd)
    {
        throw std::runtime_error("control message is not CENTRAL_FILE_END");
    }

    CentralFileEndMessage message;
    message.transfer_id=reader.u64();
    message.section_id=reader.u32();
    message.total_blocks=reader.u32();
    return message;
}

inline std::vector<std::uint8_t>encode_central_status(
    const CentralStatusMessage&message)
    {

    PacketWriter writer(60);
    write_control_header(writer,ControlType::CentralStatus);
    writer.u64(message.transfer_id);
    writer.u16(static_cast<std::uint16_t>(message.status));
    writer.u16(0);
    writer.u64(message.file_size);
    writer.bytes(message.sha256.data(),message.sha256.size());
    return writer.data();
}

inline CentralStatusMessage decode_central_status(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=60)
    {
        throw std::runtime_error("invalid CENTRAL_STATUS size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CentralStatus)
    {
        throw std::runtime_error("control message is not CENTRAL_STATUS");
    }

    CentralStatusMessage message;
    message.transfer_id=reader.u64();
    const auto raw_status=reader.u16();
    static_cast<void>(reader.u16());
    if(raw_status<static_cast<std::uint16_t>(CentralStatusCode::Cached)||
        raw_status>static_cast<std::uint16_t>(CentralStatusCode::Failed))
        {
        throw std::runtime_error("invalid CENTRAL_STATUS code");
    }
    message.status=static_cast<CentralStatusCode>(raw_status);
    message.file_size=reader.u64();
    reader.bytes(message.sha256.data(),message.sha256.size());
    return message;
}

inline std::vector<std::uint8_t>encode_central_session_end()
{
    PacketWriter writer(8);
    write_control_header(writer,ControlType::CentralSessionEnd);
    return writer.data();
}

inline void decode_central_session_end(
    const std::vector<std::uint8_t>&frame)
    {

    if(frame.size()!=8)
    {
        throw std::runtime_error("invalid CENTRAL_SESSION_END size");
    }
    PacketReader reader(frame.data(),frame.size());
    if(read_control_header(reader)!=ControlType::CentralSessionEnd)
    {
        throw std::runtime_error(
            "control message is not CENTRAL_SESSION_END");
    }
}

inline bool bitmap_test(
    const std::vector<std::uint8_t>&bitmap,
    std::uint32_t block_id)
    {

    const auto byte_index=static_cast<std::size_t>(block_id/8U);
    const auto bit_index=static_cast<unsigned int>(block_id%8U);
    if(byte_index>=bitmap.size())
    {
        throw std::runtime_error("bitmap index out of range");
    }
    return (bitmap[byte_index] & static_cast<std::uint8_t>(1U<<bit_index))!=0;
}

inline void bitmap_set(
    std::vector<std::uint8_t>&bitmap,
    std::uint32_t block_id)
    {

    const auto byte_index=static_cast<std::size_t>(block_id/8U);
    const auto bit_index=static_cast<unsigned int>(block_id%8U);
    if(byte_index>=bitmap.size())
    {
        throw std::runtime_error("bitmap index out of range");
    }
    bitmap[byte_index]|=static_cast<std::uint8_t>(1U<<bit_index);
}

inline std::size_t bitmap_size_for_blocks(std::uint32_t total_blocks)
{
    return (static_cast<std::size_t>(total_blocks)+7U)/8U;
}

inline std::uint32_t section_id_for_block(std::uint32_t block_id)
{
    return block_id/kSectionBlockCount;
}

inline std::uint32_t section_first_block(std::uint32_t section_id)
{
    return section_id * kSectionBlockCount;
}

inline std::uint32_t section_count_for_blocks(std::uint32_t total_blocks)
{
    if(total_blocks==0)
    {
        return 0;
    }
    return (total_blocks+kSectionBlockCount-1U)/kSectionBlockCount;
}

inline std::uint32_t section_block_count(
    std::uint32_t total_blocks,
    std::uint32_t section_id)
    {

    const auto first=section_first_block(section_id);
    if(first>=total_blocks)
    {
        throw std::runtime_error("section_id out of range");
    }
    return std::min(kSectionBlockCount,total_blocks-first);
}

}
