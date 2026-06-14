package http3

import (
	"bytes"
	"context"
	"fmt"
	quic "github.com/nxenon/h3spacex"
	"github.com/quic-go/qpack"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"sync"
)

// --- Added: client timing header (microseconds)
const (
	ClientTimeHeaderRTTUs = "X-Client-RTT-Us" // first-byte write -> headers parsed (TTFB-ish)
	ClientTimeHeaderPostLastUs  = "X-Client-PostLastByte-Us" // after last byte sent -> headers (true server TTFB)
)

func CalculateIntegerEncodingLengthValue(l int64) []byte {
	if l < 0 {
		panic("l is lower that 0!")
	}

	var encodedValue []byte

	switch {
	case l <= 63:
		encodedValue = []byte{byte(l)}
	case l <= 16383:
		encodedValue = []byte{0x40 | byte(l>>8), byte(l)}
	case l <= 1073741823:
		encodedValue = []byte{0x80 | byte(l>>24), byte(l >> 16), byte(l >> 8), byte(l)}
	default:
		encodedValue = []byte{0xC0 | byte(l>>56), byte(l >> 48), byte(l >> 40), byte(l >> 32),
			byte(l >> 24), byte(l >> 16), byte(l >> 8), byte(l)}
	}

	return encodedValue
}

func ParseResponseFromStream(biStream quic.Stream) (*http.Response, error) {

	frame1, err := NewXParseNextFrame(biStream, nil)
	if err != nil {
		return &http.Response{}, err
	}

	hf, ok := frame1.(*HeadersFrame)
	if !ok {
		fmt.Println("first frame is not headers frame")
		return &http.Response{}, error(nil)
	}

	headerBlock := make([]byte, hf.Length)

	if _, err2 := io.ReadFull(biStream, headerBlock); err2 != nil {
		return &http.Response{}, err2
	}
	decoder := qpack.Decoder{}

	hfs, err := decoder.DecodeFull(headerBlock)
	if err != nil {
		return &http.Response{}, err
	}

	res, err := ResponseFromHeaders(hfs)
	if err != nil {
		return &http.Response{}, err
	}

	var httpStr quic.Stream
	hstr := NewStream(biStream, nil)
	if _, ok := res.Header["Content-Length"]; ok && res.ContentLength >= 0 {
		httpStr = NewLengthLimitedStream(hstr, res.ContentLength)
	} else {
		httpStr = hstr
	}
	respBody := NewResponseBody(httpStr, nil, nil)

	_, hasTransferEncoding := res.Header["Transfer-Encoding"]
	isInformational := res.StatusCode >= 100 && res.StatusCode < 200
	isNoContent := res.StatusCode == http.StatusNoContent

	if !hasTransferEncoding && !isInformational && !isNoContent {
		res.ContentLength = -1
		if clens, ok := res.Header["Content-Length"]; ok && len(clens) == 1 {
			if clen64, err := strconv.ParseInt(clens[0], 10, 64); err == nil {
				res.ContentLength = clen64
			}
		}
	}
	requestGzip := true
	if requestGzip && res.Header.Get("Content-Encoding") == "gzip" {
		res.Header.Del("Content-Encoding")
		res.Header.Del("Content-Length")
		res.ContentLength = -1
		res.Body = NewGzipReader(respBody)
		res.Uncompressed = true
	} else {
		res.Body = respBody
	}

	return res, nil
}

func Print_bytes_in_hex(data []byte) {
	for _, n := range data {
		fmt.Printf("%02x", n) // Prints hexadecimal representation
	}
	fmt.Println()
}

func SendRequestBytesInStream(stream quic.Stream, requestBytes []byte) error {
	_, err := stream.Write(requestBytes)
	return err
}

func ReadFromAllStreams(allStreams map[*http.Request]quic.Stream) map[*http.Request]*http.Response {
	streamsResponseMap := make(map[*http.Request]*http.Response)
	for request, biStream := range allStreams {
		res, err := ReadOneStream(biStream)
		if err != nil {
			fmt.Printf("Stream ID: %d has error (no response)!: %s", biStream.StreamID(), err)
			continue
		}
		streamsResponseMap[request] = res
	}
	return streamsResponseMap
}

// --- Added: non-draining, stamps RTT (microseconds) on the response header.
func ReadFromAllStreamsTimed(
	allStreams map[*http.Request]quic.Stream,
	startTimes map[quic.Stream]time.Time,
) map[*http.Request]*http.Response {

	streamsResponseMap := make(map[*http.Request]*http.Response)

	for request, biStream := range allStreams {
		res, err := ReadOneStream(biStream)
		if err != nil {
			fmt.Printf("Stream ID: %d has error (no response)!: %s", biStream.StreamID(), err)
			continue
		}
		if t0, ok := startTimes[biStream]; ok {
			us := time.Since(t0).Microseconds()
			res.Header.Set(ClientTimeHeaderRTTUs, strconv.FormatInt(us, 10))
		}
		streamsResponseMap[request] = res
	}

	return streamsResponseMap
}

func ReadOneStream(biStream quic.Stream) (*http.Response, error) {
	res, err := ParseResponseFromStream(biStream)
	return res, err
}

func GetBidirectionalStream(quicConnection quic.Connection) quic.Stream {
	context := context.Background()
	biStream, err := quicConnection.OpenStreamSync(context)
	if err != nil {
		panic(err)
	}

	return biStream

}

func CloseAllStreams(allStreams map[*http.Request]quic.Stream) {
	for key := range allStreams {
		_ = allStreams[key].Close()
	}
}

func GetDataFrameBytes(req http.Request) []byte {

	length := CalculateIntegerEncodingLengthValue(req.ContentLength)
	buf := []byte{
		0x00,
	}
	buf = append(buf, length...)

	var requestBodyBuffer bytes.Buffer
	_, err := io.Copy(&requestBodyBuffer, req.Body)
	if err != nil {
		panic(err)
	}

	buf = append(buf, requestBodyBuffer.Bytes()...)

	return buf

}

func GetDataFrameBytesWithLengthMinusLastByteNum(req http.Request, lastByteNum int) []byte {

	length := CalculateIntegerEncodingLengthValue(req.ContentLength - int64(lastByteNum))
	buf := []byte{
		0x00,
	}
	buf = append(buf, length...)

	var requestBodyBuffer bytes.Buffer
	_, err := io.Copy(&requestBodyBuffer, req.Body)
	if err != nil {
		panic(err)
	}

	buf = append(buf, requestBodyBuffer.Bytes()...)

	return buf

}

func GetLastByteDataFrame(b []byte) []byte {

	length := CalculateIntegerEncodingLengthValue(int64(len(b)))
	buf := []byte{
		0x00,
	}
	buf = append(buf, length...)

	buf = append(buf, b...)

	return buf

}

func GetLastByteZeroDataFrameForGetRequests() []byte {

	length := []byte{byte(0)}
	buf := []byte{
		0x00,
	}
	buf = append(buf, length...)

	return buf

}

func GetRequestHeadersBytes(req http.Request, setContentLength bool) []byte {
	buf := &bytes.Buffer{}
	requestWriter := NewXRequestWriter(nil)
	isGzipped := true
	err := requestWriter.NewXwriteHeaders(buf, &req, isGzipped, setContentLength)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func GetRequestFinalPayload(req http.Request) []byte {
	setContentLength := true
	var requestDataBytes []byte
	if req.Body != nil {
		requestDataBytes = GetDataFrameBytes(req)
	}

	finalPayload := append(GetRequestHeadersBytes(req, setContentLength), requestDataBytes...)

	return finalPayload
}

func GetRequestObject(urlString string, method string, headersMap map[string]string, bodyData []byte) (http.Request, error) {
	method = strings.ToUpper(method)
	var req *http.Request
	if method == "GET" {
		reqx, err := http.NewRequest(method, urlString, nil)
		if err != nil {
			return http.Request{}, err
		}
		req = reqx
	} else {
		reqx, err2 := http.NewRequest(method, urlString, bytes.NewReader(bodyData))
		if err2 != nil {
			return http.Request{}, err2
		}
		req = reqx
	}
	for key, value := range headersMap {
		req.Header.Set(key, value)
	}
	return *req, nil
}

func GetRequestObjectGetWithBody(urlString string, method string, headersMap map[string]string, bodyData []byte) (http.Request, error) {
	method = strings.ToUpper(method)
	var req *http.Request

	reqx, err2 := http.NewRequest(method, urlString, bytes.NewReader(bodyData))
	if err2 != nil {
		return http.Request{}, err2
	}
	req = reqx

	for key, value := range headersMap {
		req.Header.Set(key, value)
	}
	return *req, nil
}

func SendLastBytesOfStreams(allStreamsWithLastByte map[quic.Stream][]byte) {
	for s, b := range allStreamsWithLastByte {
		err := SendRequestBytesInStream(s, b)
		if err != nil {
			fmt.Printf("Error occurred in sending last byte of Stream ID: %d -> %s\n", s.StreamID(), err)
		}
	}
}

// SendRequestsWithLastFrameSynchronizationMethod (concurrent reader version)
// - Stamps X-Client-RTT-Us (first write -> headers parsed)
// - Stamps X-Client-PostLastByte-Us (after last byte write -> headers parsed)
// - Starts reading immediately after sending the last byte for each stream,
//   so PostLastByte is not inflated by "close then read" overhead.
func SendRequestsWithLastFrameSynchronizationMethod(
    quicConn quic.Connection,
    allRequests []*http.Request,
    lastByteNum int,
    sleepMillisecondsBeforeSendingLastByte int,
    setContentLength bool,
) map[*http.Request]*http.Response {

    type lastChunk struct {
        req *http.Request
        s   quic.Stream
        b   []byte
    }

    out := make(map[*http.Request]*http.Response)
    var mu sync.Mutex
    var wg sync.WaitGroup

    startPre  := make(map[quic.Stream]time.Time) // before first write
    startPost := make(map[quic.Stream]time.Time) // immediately after FIN (last write + Close)

    // 1) Open streams, send all-but-last, record startPre
    var chunks []lastChunk
    for _, request := range allRequests {
        headersFrameByte := GetRequestHeadersBytes(*request, setContentLength)

        var firstPayload []byte
        var finalData []byte
        hasBody := request.Body != nil

        if lastByteNum > 0 && hasBody {
            dataFrameBytes := GetDataFrameBytesWithLengthMinusLastByteNum(*request, lastByteNum)
            allDataBytesExceptLast := dataFrameBytes[:len(dataFrameBytes)-lastByteNum]
            firstPayload = append(headersFrameByte, allDataBytesExceptLast...)

            finalByte := dataFrameBytes[len(dataFrameBytes)-lastByteNum:]
            finalData = GetLastByteDataFrame(finalByte)
        } else if hasBody {
            // no split: headers + whole body in one go
            firstPayload = append(headersFrameByte, GetDataFrameBytes(*request)...)
        } else {
            firstPayload = headersFrameByte
        }

        s := GetBidirectionalStream(quicConn)

        // pre: before first write
        startPre[s] = time.Now()
        if err := SendRequestBytesInStream(s, firstPayload); err != nil {
            fmt.Printf("Error sending initial bytes on Stream %d: %v\n", s.StreamID(), err)
            continue
        }

        if lastByteNum > 0 && hasBody {
            // we will send last byte later
            chunks = append(chunks, lastChunk{req: request, s: s, b: finalData})
        } else {
            // no last chunk: send FIN now and start reading
            if err := s.Close(); err != nil {
                fmt.Printf("Error closing (FIN) Stream %d: %v\n", s.StreamID(), err)
                // still attempt to read
            }
            startPost[s] = time.Now()

            wg.Add(1)
            go func(req *http.Request, st quic.Stream) {
                defer wg.Done()
                res, err := ReadOneStream(st)
                if err != nil {
                    fmt.Printf("Stream %d read error: %v\n", st.StreamID(), err)
                    return
                }
                if t0, ok := startPre[st]; ok {
                    res.Header.Set(ClientTimeHeaderRTTUs, strconv.FormatInt(time.Since(t0).Microseconds(), 10))
                }
                if t1, ok := startPost[st]; ok {
                    res.Header.Set(ClientTimeHeaderPostLastUs, strconv.FormatInt(time.Since(t1).Microseconds(), 10))
                }
                mu.Lock(); out[req] = res; mu.Unlock()
            }(request, s)
        }
    }

    // 2) Sleep before sending last bytes (if any)
    if lastByteNum > 0 {
        time.Sleep(time.Duration(sleepMillisecondsBeforeSendingLastByte) * time.Millisecond)
    }

    // 3) Send last bytes, CLOSE (FIN) immediately, then start reading
    for _, c := range chunks {
        if err := SendRequestBytesInStream(c.s, c.b); err != nil {
            fmt.Printf("Error sending last byte on Stream %d: %v\n", c.s.StreamID(), err)
            continue
        }
        if err := c.s.Close(); err != nil {
            fmt.Printf("Error closing (FIN) Stream %d: %v\n", c.s.StreamID(), err)
            // continue anyway
        }
        startPost[c.s] = time.Now()

        wg.Add(1)
        go func(req *http.Request, st quic.Stream) {
            defer wg.Done()
            res, err := ReadOneStream(st)
            if err != nil {
                fmt.Printf("Stream %d read error: %v\n", st.StreamID(), err)
                return
            }
            if t0, ok := startPre[st]; ok {
                res.Header.Set(ClientTimeHeaderRTTUs, strconv.FormatInt(time.Since(t0).Microseconds(), 10))
            }
            if t1, ok := startPost[st]; ok {
                res.Header.Set(ClientTimeHeaderPostLastUs, strconv.FormatInt(time.Since(t1).Microseconds(), 10))
            }
            mu.Lock(); out[req] = res; mu.Unlock()
        }(c.req, c.s)
    }

    // 4) Wait for all readers to finish
    wg.Wait()
    return out
}


// Stamps both timings on each response without draining bodies.
func readFromAllStreamsStampTimes(
    allStreams map[*http.Request]quic.Stream,
    startPre  map[quic.Stream]time.Time,
    startPost map[quic.Stream]time.Time,
) map[*http.Request]*http.Response {

    out := make(map[*http.Request]*http.Response)
    for req, s := range allStreams {
        res, err := ReadOneStream(s)
        if err != nil {
            fmt.Printf("Stream ID: %d has error (no response)!: %s", s.StreamID(), err)
            continue
        }
        if t0, ok := startPre[s]; ok {
            res.Header.Set(ClientTimeHeaderRTTUs,
                strconv.FormatInt(time.Since(t0).Microseconds(), 10))
        }
        if t1, ok := startPost[s]; ok {
            res.Header.Set(ClientTimeHeaderPostLastUs,
                strconv.FormatInt(time.Since(t1).Microseconds(), 10))
        }
        out[req] = res
    }
    return out
}

// SendRequestsWithLastFrameSynchronizationMethodSerialized
// - Opens N streams, sends headers+body-minus-last for all.
// - Then releases EXACTLY ONE stream at a time: send last byte + FIN, block until headers.
// - Stamps both timing headers (µs) without draining bodies.
func SendRequestsWithLastFrameSynchronizationMethodSerialized(
    quicConn quic.Connection,
    allRequests []*http.Request,
    lastByteNum int,
    initialHoldMs int,        // small wait to let "first parts" reach server (e.g., 5–50ms)
    interReleaseMs int,       // optional spacing between releases (e.g., 0–5ms)
    setContentLength bool,
) map[*http.Request]*http.Response {

    type streamState struct {
        req       *http.Request
        s         quic.Stream
        lastChunk []byte
        hasLast   bool
    }

    out := make(map[*http.Request]*http.Response)
    states := make([]streamState, 0, len(allRequests))

    startPre  := make(map[quic.Stream]time.Time) // before first write
    startPost := make(map[quic.Stream]time.Time) // just after FIN

    // 1) Prime: open all streams and send all-but-last
    for _, request := range allRequests {
        headers := GetRequestHeadersBytes(*request, setContentLength)

        var firstPayload []byte
        var lastChunk []byte
        hasBody := request.Body != nil

        if lastByteNum > 0 && hasBody {
            data := GetDataFrameBytesWithLengthMinusLastByteNum(*request, lastByteNum)
            // all except the last "lastByteNum" bytes
            firstPayload = append(headers, data[:len(data)-lastByteNum]...)
            final := data[len(data)-lastByteNum:]
            lastChunk = GetLastByteDataFrame(final)
        } else if hasBody {
            firstPayload = append(headers, GetDataFrameBytes(*request)...)
        } else {
            firstPayload = headers
        }

        s := GetBidirectionalStream(quicConn)
        startPre[s] = time.Now()
        if err := SendRequestBytesInStream(s, firstPayload); err != nil {
            fmt.Printf("Error sending first part on Stream %d: %v\n", s.StreamID(), err)
            continue
        }

        st := streamState{req: request, s: s, lastChunk: lastChunk, hasLast: (lastByteNum > 0 && hasBody)}
        states = append(states, st)

        if !st.hasLast {
            // no last chunk: close now so the server can process
            _ = s.Close()
            startPost[s] = time.Now()
            res, err := ReadOneStream(s)
            if err != nil {
                fmt.Printf("Stream %d read error: %v\n", s.StreamID(), err)
                continue
            }
            if t0, ok := startPre[s]; ok {
                res.Header.Set(ClientTimeHeaderRTTUs, strconv.FormatInt(time.Since(t0).Microseconds(), 10))
            }
            if t1, ok := startPost[s]; ok {
                res.Header.Set(ClientTimeHeaderPostLastUs, strconv.FormatInt(time.Since(t1).Microseconds(), 10))
            }
            out[request] = res
        }
    }

    if initialHoldMs > 0 {
        time.Sleep(time.Duration(initialHoldMs) * time.Millisecond)
    }

    // 2) Release exactly one stream at a time: last byte + FIN, then read headers synchronously
    for _, st := range states {
        if !st.hasLast {
            continue // already done above
        }
        if err := SendRequestBytesInStream(st.s, st.lastChunk); err != nil {
            fmt.Printf("Error sending last chunk on Stream %d: %v\n", st.s.StreamID(), err)
            continue
        }
        // FIN so the server starts work
        _ = st.s.Close()
        startPost[st.s] = time.Now()

        // Block until headers parsed for THIS stream
        res, err := ReadOneStream(st.s)
        if err != nil {
            fmt.Printf("Stream %d read error: %v\n", st.s.StreamID(), err)
            continue
        }
        if t0, ok := startPre[st.s]; ok {
            res.Header.Set(ClientTimeHeaderRTTUs, strconv.FormatInt(time.Since(t0).Microseconds(), 10))
        }
        if t1, ok := startPost[st.s]; ok {
            res.Header.Set(ClientTimeHeaderPostLastUs, strconv.FormatInt(time.Since(t1).Microseconds(), 10))
        }
        out[st.req] = res

        if interReleaseMs > 0 {
            time.Sleep(time.Duration(interReleaseMs) * time.Millisecond)
        }
    }

    return out
}
