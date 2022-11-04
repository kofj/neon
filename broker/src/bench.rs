use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use neon_broker::neon_broker_proto::neon_broker_client::NeonBrokerClient;
use neon_broker::neon_broker_proto::subscribe_safekeeper_info_request::SubscriptionKey;
use neon_broker::neon_broker_proto::TenantTimelineId as ProtoTenantTimelineId;
use neon_broker::neon_broker_proto::{SafekeeperTimelineInfo, SubscribeSafekeeperInfoRequest};
use neon_broker::DEFAULT_LISTEN_ADDR;
use tokio::time::{self};

use tonic::transport::Channel;
use tonic::Request;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of publishers
    #[clap(short = 'p', long, value_parser, default_value_t = 1)]
    num_pubs: u64,
    /// Number of subscribers
    #[clap(short = 's', long, value_parser, default_value_t = 1)]
    num_subs: u64,
}

async fn progress_reporter(counters: Vec<Arc<AtomicU64>>) {
    let mut interval = time::interval(Duration::from_millis(1000));
    let mut c_old = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let mut c_min_old = counters
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .min()
        .unwrap_or(0);
    let mut started_at = None;
    let mut skipped: u64 = 0;
    loop {
        interval.tick().await;
        // print!(
        //     "cnts are {:?}",
        //     counters
        //         .iter()
        //         .map(|c| c.load(Ordering::Relaxed))
        //         .collect::<Vec<_>>()
        // );
        let c_new = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        let c_min_new = counters
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .min()
            .unwrap_or(0);
        if c_new > 0 && started_at.is_none() {
            started_at = Some(Instant::now());
            skipped = c_new;
        }
        let avg_rps = started_at.map(|s| {
            let dur = s.elapsed();
            let dur_secs = dur.as_secs() as f64 + (dur.subsec_millis() as f64) / 1000.0;
            let avg_rps = (c_new - skipped) as f64 / dur_secs;
            (dur, avg_rps)
        });
        println!(
            "sum rps {}, min rps {} total {}, total min {}, duration, avg sum rps {:?}",
            c_new - c_old,
            c_min_new - c_min_old,
            c_new,
            c_min_new,
            avg_rps
        );
        c_old = c_new;
        c_min_old = c_min_new;
    }
}

fn tli_from_u64(i: u64) -> Vec<u8> {
    let mut timeline_id = vec![0xFF; 8];
    timeline_id.extend_from_slice(&i.to_be_bytes());
    timeline_id
}

async fn subscribe(client: Option<NeonBrokerClient<Channel>>, counter: Arc<AtomicU64>, i: u64) {
    let mut client = match client {
        Some(c) => c,
        None => NeonBrokerClient::connect("http://[::1]:50051")
            .await
            .unwrap(),
    };

    // let key = SubscriptionKey::All(Empty {});
    let key = SubscriptionKey::TenantTimelineId(ProtoTenantTimelineId {
        tenant_id: vec![0xFF; 16],
        timeline_id: tli_from_u64(i),
    });
    let request = SubscribeSafekeeperInfoRequest {
        subscription_key: Some(key),
    };
    let mut stream = client
        .subscribe_safekeeper_info(request)
        .await
        .unwrap()
        .into_inner();

    while let Some(_feature) = stream.message().await.unwrap() {
        counter.fetch_add(1, Ordering::Relaxed);
        // println!("info = {:?}, client {}", _feature, i);
    }
}

async fn publish(client: Option<NeonBrokerClient<Channel>>, n_keys: u64) {
    let mut client = match client {
        Some(c) => c,
        None => NeonBrokerClient::connect("http://[::1]:50051")
            .await
            .unwrap(),
    };
    let mut counter: u64 = 0;

    // create stream producing new values
    let outbound = async_stream::stream! {
        loop {
            let info = SafekeeperTimelineInfo {
                safekeeper_id: 1,
                tenant_timeline_id: Some(ProtoTenantTimelineId {
                    tenant_id: vec![0xFF; 16],
                    timeline_id: tli_from_u64(counter % n_keys),
                }),
                last_log_term: 0,
                flush_lsn: counter,
                commit_lsn: 2,
                backup_lsn: 3,
                remote_consistent_lsn: 4,
                peer_horizon_lsn: 5,
                safekeeper_connstr: "zenith-1-sk-1.local:7676".to_owned(),
                local_start_lsn: 0,
            };
            counter += 1;
            // println!("sending info = {:?}", info);
            if counter >= 10 {
                break;
            }
            yield info;
            time::sleep(Duration::from_millis(1000)).await;
        }
    };
    let response = client.publish_safekeeper_info(Request::new(outbound)).await;
    println!("pub response is {:?}", response);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut counters = Vec::with_capacity(args.num_subs as usize);
    for _ in 0..args.num_subs {
        counters.push(Arc::new(AtomicU64::new(0)));
    }
    let h = tokio::spawn(progress_reporter(counters.clone()));

    let c = NeonBrokerClient::connect(format!("http://{}", DEFAULT_LISTEN_ADDR))
        .await
        .unwrap();

    for i in 0..args.num_subs {
        let c = Some(c.clone());
        // let c = None;
        tokio::spawn(subscribe(c, counters[i as usize].clone(), i));
    }
    for _i in 0..args.num_pubs {
        // let c = Some(c.clone());
        tokio::spawn(publish(None, args.num_subs as u64));
    }

    h.await?;
    Ok(())
}
