//!
//! ```
//! mkdir actcast
//! # put AmazonRootCA1.pem to ./actcast
//! # put device_rsa_key to ./actcast
//! # put cert.pem to ./actcast
//! #   e.g. jq -r '.cert' /var/actcast/initial_settings > ./actcast/cert.pem
//! # set thing_name in this source code
//! #   e.g. thing_name=$(jq -r '.thing_name' /var/actcast/initial_settings)
//! #        sed -i -e "s|thing_name|${thing_name}|" $0.rs
//! export RUST_LOG=trace
//! export MQTT_C_CLIENT_TRACE=ON
//! export MQTT_C_CLIENT_TRACE_LEVEL=MAXIMUM
//! cargo run
//! ```
//!

use chrono::{serde::ts_milliseconds, DateTime, Duration, Utc};
use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use serde::{Deserialize, Serialize};
use serde_json;
use std::{path::PathBuf, process};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq)]
pub struct ThingName(pub String);
#[derive(Serialize, Deserialize, Debug)]
pub struct ActLog {
    pub thing_name: ThingName,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
    pub act_id: ActId,
    pub sequence_number: u64,
    pub act_log: Vec<serde_json::Value>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct SysLog {
    pub thing_name: String,
    pub payload: String,
    // syslog loglevel [0, 7]
    pub level: i8,
    pub sequence_number: u64,
    pub boot_id: String,
    pub session_id: String,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
}
/// Any event raised from device(and Act)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventLog {
    pub event: Event,
    #[serde(with = "ts_milliseconds")]
    pub timestamp: DateTime<Utc>,
    pub sequence_number: u64,
    pub boot_id: String,
    pub session_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum Event {
    // DeviceStatus {
    //     #[serde(flatten)]
    //     status: DeviceStatus,
    //     #[serde(with = "format::duration::DurationDelegate")]
    //     uptime: Duration,
    // },
    AgentStarted {
        firmware_version: String,
        os_version: String,
        kernel_version: String,
        hardware_id: String,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    AgentStopping {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    AgentRerunning {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    OsRebooting {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    OsShutdowning {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActDownloading {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActDownloaded {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActStarted {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActStopping {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActStopped {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    FirmwareUpdating {
        firmware_version: Option<String>,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    FirmwareUpdated {
        success: bool,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    AgentRuntimeError {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActRuntimeError {
        act_id: ActId,
        status: ActError,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActUnhealthy {
        act_id: ActId,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    ActInstallError {
        act_id: ActId,
        should_retry: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        r#type: Option<ActInstallErrorType>,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    PhotoUploading {
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
    PhotoUploaded {
        success: bool,
        #[serde(with = "format::duration::DurationDelegate")]
        uptime: Duration,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord)]
#[serde(tag = "error_type", rename_all = "snake_case")]
pub enum ActInstallErrorType {
    DeviceUnavailable { device: String },
    /* NotChangedHealthy */
    /* UnCategorised */
}

mod format {
    use serde::{Deserialize, Serialize};

    pub mod duration {
        use super::*;
        use chrono::Duration;

        #[derive(Serialize, Deserialize)]
        #[serde(remote = "Duration")]
        pub struct DurationDelegate(#[serde(getter = "Duration::num_seconds")] i64);

        // Provide a conversion to construct the remote type.
        impl From<DurationDelegate> for Duration {
            fn from(def: DurationDelegate) -> Duration {
                Duration::seconds(def.0)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ActError {
    /// exit code
    #[serde(rename = "act_exited")]
    Exited { exited: i32 },
    /// OOM event occurs.
    /// It does not necessarily mean that the container has stopped.
    #[serde(rename = "act_oomkilled")]
    OomKilled,
}
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActId(pub u64);

fn main() -> mqtt::Result<()> {
    // Initialize the logger from the environment
    env_logger::init();

    // We use the trust store from the Paho C tls-testing/keys directory,
    // but we assume there's a copy in the current directory.
    // ca_path
    const TRUST_STORE: &str = "AmazonRootCA1.pem";

    // cert_path
    const KEY_STORE: &str = "cert.pem";

    // We assume that we are in a valid directory.
    let mut trust_store = PathBuf::from("actcast");
    trust_store.push(TRUST_STORE);

    let mut key_store = PathBuf::from("actcast");
    key_store.push(KEY_STORE);

    if !trust_store.exists() {
        println!("The trust store file does not exist: {:?}", trust_store);
        println!("  Get a copy from \"paho.mqtt.c/test/ssl/{}\"", TRUST_STORE);
        process::exit(1);
    }

    if !key_store.exists() {
        println!("The key store file does not exist: {:?}", key_store);
        println!("  Get a copy from \"paho.mqtt.c/test/ssl/{}\"", KEY_STORE);
        process::exit(1);
    }

    let private_key = "actcast/device_rsa_key";
    let thing_name = "62ca3fbf-a316-48f2-906d-51ee35ad66dd".to_string();

    // Let the user override the host, but note the "ssl://" protocol.
    let host = "ssl://a1sgglpp228nnc-ats.iot.ap-northeast-1.amazonaws.com:443".to_string();

    println!("Connecting to host: '{}'", host);

    // Run the client in an async bloc

    // let pool = ThreadPool::new().unwrap();

    let publisher = async {
        // Create a client & define connect options
        let cli = mqtt::CreateOptionsBuilder::new()
            .server_uri(&host)
            .client_id(&thing_name)
            .max_buffered_messages(1024)
            .send_while_disconnected(true)
            .allow_disconnected_send_at_anytime(true)
            .delete_oldest_messages(true)
            .persistence(mqtt::PersistenceType::None)
            .mqtt_version(4)
            .create_client()?;

        let alpn = vec!["x-amzn-mqtt-ca"];
        let ssl_opts = mqtt::SslOptionsBuilder::new()
            .key_store(key_store)?
            .private_key(private_key)?
            .ssl_version(mqtt::ssl_options::SslVersion::Tls_1_2)
            .verify(true)
            .trust_store(trust_store)?
            .alpn_protos(&alpn)
            .enable_server_cert_auth(true)
            .finalize();

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .retry_interval(Duration::seconds(5).to_std().unwrap())
            .clean_session(false)
            .ssl_options(ssl_opts)
            .finalize();

        let recver = std::thread::Builder::new()
            .name("recver".to_string())
            .spawn({
                let mut cli = cli.clone();
                || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                        .unwrap();
                    rt.block_on(async move {
                        let mut strm = cli.get_stream(25);
                        while let Some(m) = strm.next().await {
                            if let Some(m) = m {
                                println!("recv: {:?}", m);
                            } else {
                                println!("recv: disconnect");
                                let re = cli.reconnect().wait();
                                println!("reconnect: {:?}", re);
                            }
                        }
                        println!("recv end");
                        mqtt::Result::Ok(())
                    })
                }
            })
            .unwrap();

        cli.connect(conn_opts).await?;

        let syslog_topic = format!(
            "$aws/rules/actcast_stg_iot_syslog_rule/things/{}/syslog",
            thing_name
        );
        let eventlog_topic = format!(
            "$aws/rules/actcast_stg_iot_event_rule/things/{}/eventlog",
            thing_name
        );

        let payload: String = {
            // a payload larger than 128KB which is size limit of AWSIoT
            let mut v = vec![];
            v.push(0);
            for _ in 0..128 * 1024 {
                v.push(1);
            }
            v.push(2);
            String::from_utf8_lossy(&v).into_owned()
        };
        // let log = SysLog {
        //     thing_name,
        //     // payload: "Hello secure world!".to_string(),
        //     payload,
        //     level: 0,
        //     sequence_number: 0,
        //     boot_id: "0".to_string(),
        //     session_id: "0".to_string(),
        //     timestamp: Utc::now(),
        // };
        let log = EventLog {
            event: Event::ActInstallError {
                act_id: ActId(0),
                should_retry: false,
                r#type: Some(ActInstallErrorType::DeviceUnavailable {
                    device: "/dev/video0".to_string(),
                }),
                uptime: Duration::seconds(256),
            },
            timestamp: Utc::now(),
            sequence_number: 0,
            boot_id: "0".to_string(),
            session_id: "0".to_string(),
        };
        let msg = mqtt::MessageBuilder::new()
            .topic(eventlog_topic)
            .payload(serde_json::to_vec(&log).unwrap())
            .qos(1)
            .finalize();

        println!("publishing...");
        cli.publish(msg).await?;
        println!("published");

        println!("disconnecting...");
        cli.disconnect(None).await?;
        println!("disconnected");

        println!("joining...");
        let res = recver.join();
        println!("joined: {:?}", res);
        mqtt::Result::Ok(())
    };
    block_on(publisher)
}
