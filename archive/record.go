package archive

type RecordSet struct {
	data []map[string]interface{}
}

func createRecordSet() *RecordSet {
	return &RecordSet{data: make([]map[string]interface{}, 0)}
}

func (rs *RecordSet) Add(record map[string]interface{}) {
	rs.data = append(rs.data, record)
}

func (rs *RecordSet) Data() []map[string]interface{} {
	return rs.data
}

func (rs *RecordSet) ForEach(fn func(map[string]interface{}) error) error {
	for _, record := range rs.data {
		err := fn(record)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *RecordSet) Count() int {
	return len(rs.data)
}

func (rs *RecordSet) Get(index int) map[string]interface{} {
	if rs.data[index] != nil {
		return rs.data[index]
	}

	var x map[string]interface{}
	return x
}
