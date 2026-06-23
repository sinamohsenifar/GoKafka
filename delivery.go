package gokafka

// ProduceRecordResult is the broker acknowledgement for a produced record.
type ProduceRecordResult struct {
	Record    Record
	Topic     string
	Partition int32
	Offset    int64
}
