package gokafka

// SetHeader sets or replaces a record header by key.
func (r *Record) SetHeader(key string, value []byte) {
	for i := range r.Headers {
		if r.Headers[i].Key == key {
			r.Headers[i].Value = value
			return
		}
	}
	r.Headers = append(r.Headers, Header{Key: key, Value: value})
}

// GetHeader returns a header value and whether the key exists.
func (r *Record) GetHeader(key string) ([]byte, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}

// WithHeaders returns a copy of the record with additional headers (later keys override).
func (r Record) WithHeaders(headers ...Header) Record {
	out := r
	m := map[string][]byte{}
	order := make([]string, 0, len(r.Headers)+len(headers))
	for _, h := range r.Headers {
		if _, ok := m[h.Key]; !ok {
			order = append(order, h.Key)
		}
		m[h.Key] = h.Value
	}
	for _, h := range headers {
		if _, ok := m[h.Key]; !ok {
			order = append(order, h.Key)
		}
		m[h.Key] = h.Value
	}
	out.Headers = make([]Header, len(order))
	for i, k := range order {
		out.Headers[i] = Header{Key: k, Value: m[k]}
	}
	return out
}

// HeaderRecord builds a record with headers for produce calls.
func HeaderRecord(topic string, value []byte, headers ...Header) Record {
	return Record{Topic: topic, Value: value, Headers: headers}
}
