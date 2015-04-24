import RippledLogMonitor from './lib/rippled_log_monitor'
import Hbase from 'hbase'
import StatsD from 'node-statsd'

const RIPPLED_LOG_PATH = process.env.RIPPLED_LOG_PATH || '/var/log/rippled/debug.log'

class HbaseValidationLogger extends RippledLogMonitor {
  constructor() {
    this.client = Hbase({
      host: process.env.HBASE_HOST,
      port: process.env.HBASE_PORT
    })
    super()
  }
  // @override
  onValidation(entry) {
    this.client.table(process.env.HBASE_HOST)
      // Use "public_key|ledger_hash" as row name
      .row(`${entry.public_key}|${entry.hash}`)
      .put('validation:public_key', entry.public_key, (err, success) => {
        if (err) {
          return console.error('hbase put error', err)
        }
        this.put('validation:hash', entry.hash, (err, success) => {
          if (err) {
            return console.error('hbase put error', err)
          }
          this.put('validation:datetime', entry.datetime, (err, success) => {
            if (err) {
              return console.error('hbase put error', err)
            }
            console.log('saved validation in hbase', success);
          });
        });
      });
  }
}

class StatsdValidationLogger extends RippledLogMonitor {
  constructor() {
    this.client = new StatsD({
      host: process.env.STATSD_HOST,
      port: process.env.STATSD_PORT
    })
    super()
  }
  // @override
  onValidation(entry) {
    this.client.increment(entry.public_key, 1)
  }
}

let monitor = new HbaseValidationLogger()
let statsdMonitor = new StatsdValidationLogger()

monitor.monitorFile(RIPPLED_LOG_PATH)
statsdMonitor.monitorFile(RIPPLED_LOG_PATH)
