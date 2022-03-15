package pkg

type config struct {
	clickhouse clickHouseConfig
	task       *taskConfig
}

type clickHouseConfig struct {
	db string

	retryTimes int
}

type taskConfig struct {
	name string

	tableName string

	dims []struct {
		name       string
		typ        string
		sourceName string
	}
}
