//go:build windows

package dataplane

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"netmesh/internal/protocol"
)

var (
	iphlpapi            = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile  = iphlpapi.NewProc("IcmpCreateFile")
	procIcmpCloseHandle = iphlpapi.NewProc("IcmpCloseHandle")
	procIcmpSendEcho    = iphlpapi.NewProc("IcmpSendEcho")
)

const ipSuccess = 0

type icmpProber struct{}

type ipOptionInformation struct {
	TTL         uint8
	TOS         uint8
	Flags       uint8
	OptionsSize uint8
	OptionsData uintptr
}

type icmpEchoReply struct {
	Address       uint32
	Status        uint32
	RoundTripTime uint32
	DataSize      uint16
	Reserved      uint16
	Data          uintptr
	Options       ipOptionInformation
}

// Probe uses Windows' ICMP helper API instead of a Go ICMP packet socket. The
// packet socket path used on Unix reports WSAEPROTONOSUPPORT on Windows.
func (icmpProber) Probe(ctx context.Context, agentID string, flow protocol.AgentFlow, spec protocol.TestSpec) protocol.Metric {
	m := protocol.Metric{Target: flow.DstAddr, TS: time.Now().UnixMilli()}
	host := flow.DstAddr
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	m.Target = host

	ipAddr, err := net.ResolveIPAddr("ip4", host)
	if err != nil {
		m.Err = "resolve: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	ip4 := ipAddr.IP.To4()
	if ip4 == nil {
		m.Err = "resolve: no IPv4 address for " + host
		m.PacketLoss = 100
		return m
	}
	m.RemoteAddr = ip4.String()

	handle, err := icmpCreateFile()
	if err != nil {
		m.Err = "icmp unavailable: " + err.Error()
		m.PacketLoss = 100
		return m
	}
	defer icmpCloseHandle(handle)

	size := payloadSize(spec, 56)
	timeoutMS := uint32(probeTimeout / time.Millisecond)
	dst := binary.LittleEndian.Uint32(ip4)
	var br burstResult
	var lastErr string
	for seq := 0; seq < probeBurst; seq++ {
		select {
		case <-ctx.Done():
			lastErr = ctx.Err().Error()
			br.apply(&m, probeBurst)
			if !m.Success {
				m.Err = lastErr
			}
			return m
		default:
		}

		payload := encodeProbe(uint64(seq), size)
		reply, data, err := icmpSendEcho(handle, dst, payload, timeoutMS)
		if err != nil {
			lastErr = err.Error()
			continue
		}
		if reply.Status != ipSuccess {
			lastErr = fmt.Sprintf("icmp status %d", reply.Status)
			continue
		}
		if _, sendNs, ok := decodeProbe(data); ok {
			br.received++
			if reply.RoundTripTime > 0 {
				br.rttSumNs += int64(reply.RoundTripTime) * int64(time.Millisecond)
			} else {
				br.rttSumNs += time.Now().UnixNano() - sendNs
			}
			if m.TTL == 0 {
				m.TTL = int(reply.Options.TTL)
			}
		}
	}
	br.apply(&m, probeBurst)
	if !m.Success && lastErr != "" {
		m.Err = lastErr
	}
	return m
}

func icmpCreateFile() (windows.Handle, error) {
	r1, _, err := procIcmpCreateFile.Call()
	if r1 == uintptr(windows.InvalidHandle) || r1 == 0 {
		if err != syscall.Errno(0) {
			return 0, err
		}
		return 0, errors.New("IcmpCreateFile failed")
	}
	return windows.Handle(r1), nil
}

func icmpCloseHandle(handle windows.Handle) {
	_, _, _ = procIcmpCloseHandle.Call(uintptr(handle))
}

func icmpSendEcho(handle windows.Handle, dst uint32, payload []byte, timeoutMS uint32) (icmpEchoReply, []byte, error) {
	replySize := int(unsafe.Sizeof(icmpEchoReply{})) + len(payload) + 8
	replyBuf := make([]byte, replySize)
	r1, _, err := procIcmpSendEcho.Call(
		uintptr(handle),
		uintptr(dst),
		uintptr(unsafe.Pointer(&payload[0])),
		uintptr(uint16(len(payload))),
		0,
		uintptr(unsafe.Pointer(&replyBuf[0])),
		uintptr(uint32(len(replyBuf))),
		uintptr(timeoutMS),
	)
	if r1 == 0 {
		if err != syscall.Errno(0) {
			return icmpEchoReply{}, nil, err
		}
		return icmpEchoReply{}, nil, errors.New("IcmpSendEcho failed")
	}
	reply := *(*icmpEchoReply)(unsafe.Pointer(&replyBuf[0]))
	data := []byte(nil)
	if reply.Data != 0 && reply.DataSize > 0 {
		replyData := unsafe.Slice((*byte)(unsafe.Pointer(reply.Data)), int(reply.DataSize))
		data = append([]byte(nil), replyData...)
	}
	return reply, data, nil
}
