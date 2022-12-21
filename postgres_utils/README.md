## How to run
The postgres database writer uses a simple JSON configuration file:

```json
{
	"database": "database_name",
	"user": "postgres",
	"password": "postgres",
	"table": "table_name",
	"metrics_input": "/path/to/metrics/output"
}
```

The call format to the program from the command line would be:

`python main.py <cfg_path>`

For example:

`python main.py powertoys_cfg.json`
